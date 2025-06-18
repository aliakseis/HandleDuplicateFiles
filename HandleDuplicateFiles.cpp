#include <windows.h>
#include <tchar.h>

#include <fcntl.h>  
#include <io.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <cstring>
#include <cstdint>
#include <algorithm>

constexpr ULONGLONG MIN_SIZE_TO_CONSIDER = 16 * 1024;

// Use a suitable buffer size for file comparisons.
constexpr size_t BUFFER_SIZE = 4096;

//------------------------------------------------------------------------------
// RightFileState
//   A simple structure to hold the per-right-file state.
struct RightFileState {
    std::wstring filePath;      // The file's full path.
    std::ifstream stream;       // Opened binary stream for the file.
};

typedef std::pair<std::streamsize, char> GroupKey;

//------------------------------------------------------------------------------
// CompareFilesBufferedAdvanced()
//   Template function that compares a master (left) file against a collection
//   of right files (via iterators over std::wstring). It reads the master file
//   one chunk at a time. For each master chunk, it reads the same number of bytes
//   from each right file. If a mismatch or size difference is detected, the 
//   function computes a comparison key (for example, the first mismatch byte offset
//   plus 1, multiplied by -1 if master < right, or by 1 if master > right), adds 
//   the right file's path to the keyGroups map, and removes that file from further comparison.
//   After processing the master file, any remaining right file is checked for extra data.
template <typename T> // T is an iterator over std::wstring items.
void CompareFilesBufferedAdvanced(const std::wstring& masterFilePath,
    T rightFileBegin,
    T rightFileEnd,
    std::streamsize totalBytesRead,
    std::map<GroupKey, std::vector<std::wstring>>& keyGroups,
    std::vector<std::wstring>& duplicateGroup)
{
    // Open the master (left) file in binary mode.
    std::ifstream master(masterFilePath, std::ios::binary);
    if (!master) {
        std::wcerr << L"Error opening master file: " << masterFilePath << std::endl;
        return;
    }
    master.seekg(totalBytesRead, std::ios::beg);

    // Build a vector of right file state objects.
    std::vector<RightFileState> rightStates;
    for (T it = rightFileBegin; it != rightFileEnd; ++it)
    {
        RightFileState state;
        state.filePath = *it;
        state.stream.open(state.filePath.c_str(), std::ios::binary);
        if (!state.stream) {
            std::wcerr << L"Error opening right file: " << state.filePath << std::endl;
            continue;
        }
        state.stream.seekg(totalBytesRead, std::ios::beg);
        rightStates.push_back(std::move(state));
    }

    char masterBuffer[BUFFER_SIZE];

    //std::streamsize totalBytesRead = 0;

    // Process the master file one chunk at a time.
    while (true)
    {
        master.read(masterBuffer, BUFFER_SIZE);
        std::streamsize masterBytes = master.gcount();
        if (masterBytes <= 0) // End of master file.
            break;

        // Iterate over rightStates; if a right file fails comparison for this chunk,
        // compute its key and remove it from the list.
        for (auto it = rightStates.begin(); it != rightStates.end(); )
        {
            char rightBuffer[BUFFER_SIZE];
            it->stream.read(rightBuffer, masterBytes);
            std::streamsize rightBytes = it->stream.gcount();

            // If the right file didn't supply as many bytes as master, it's shorter or had a read error.
            if (rightBytes != masterBytes) {
                it = rightStates.erase(it);
                continue;
            }

            // Compare the current chunk.
            int cmp = std::memcmp(masterBuffer, rightBuffer, static_cast<size_t>(masterBytes));
            if (cmp != 0)
            {
                // Find first mismatching byte.
                std::streamsize mismatchIndex = 0;
                for (; mismatchIndex < masterBytes; ++mismatchIndex) {
                    if (masterBuffer[mismatchIndex] != rightBuffer[mismatchIndex])
                        break;
                }
                //int64_t diffKey = (cmp < 0 ? -1LL : 1LL) * (totalBytesRead + mismatchIndex + 1);
                GroupKey key{ totalBytesRead + mismatchIndex, rightBuffer[mismatchIndex] };
                keyGroups[key].push_back(it->filePath);
                it = rightStates.erase(it);
            }
            else {
                ++it;
            }
        }

        totalBytesRead += masterBytes;

        // If all right files have produced a difference, we're done.
        if (rightStates.empty())
            break;
    }

    // For any right file that is still in the state list (i.e. no mismatch was found in the master part),
    // check if it might have extra data.
    for (auto& state : rightStates)
    {
        // Force state update.
        state.stream.peek();
        if (state.stream.eof()) {
            // The file matches the master exactly.
            duplicateGroup.push_back(state.filePath);
        }
    }
}


//------------------------------------------------------------------------------
// GroupFilesByContentUsingMap()
//    Group files (all of same size) by content using a hash map keyed by an
//    int64_t comparison key produced against a chosen pivot.
//    Duplicate groups (with two or more files) are recorded in duplicateGroups.
void GroupFilesByContentUsingMap(const std::vector<std::wstring>& files,
    std::vector<std::vector<std::wstring>>& duplicateGroups, std::streamsize totalBytesRead)
{
    if (files.size() < 2)
        return;

    std::vector<std::wstring> duplicateGroup;

    std::map<GroupKey, std::vector<std::wstring>> keyGroups;
    // Use the first file as the pivot.
    const std::wstring& pivot = files[0];
    duplicateGroup.push_back(pivot); // the pivot is equal to itself.


    // Limit batch size in the call to CompareFilesBufferedAdvanced.
    constexpr size_t MAX_BATCH = 256;
    auto rightBegin = std::next(files.begin());
    auto rightEnd = files.end();
    size_t totalRightFiles = std::distance(rightBegin, rightEnd);
    size_t processed = 0;
    while (processed < totalRightFiles)
    {
        // Calculate the iterators for the current batch.
        auto batchBegin = rightBegin;
        std::advance(batchBegin, processed);
        size_t batchSize = (std::min)(totalRightFiles - processed, MAX_BATCH);
        auto batchEnd = batchBegin;
        std::advance(batchEnd, batchSize);

        CompareFilesBufferedAdvanced(pivot, batchBegin, batchEnd, totalBytesRead, keyGroups, duplicateGroup);
        processed += batchSize;
    }

    // Group with key 0 are duplicates of pivot.
    if (duplicateGroup.size() > 1)
        duplicateGroups.push_back(duplicateGroup);

    // Process nonzero key groups recursively.
    for (const auto& entry : keyGroups)
    {
        //if (entry.first == 0)
        //    continue;
        if (entry.second.size() > 1)
            GroupFilesByContentUsingMap(entry.second, duplicateGroups, entry.first.first);
    }
}

//------------------------------------------------------------------------------
// ToLower()
//    Converts a std::wstring to lower-case.
std::wstring ToLower(const std::wstring& str)
{
    std::wstring lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::towlower);
    return lower;
}

//------------------------------------------------------------------------------
// HasExtension()
//    Checks whether the file (by its full path) has an extension that matches (case-insensitive)
//    the provided extension filter. The extension filter should include the dot (e.g. ".txt").
//    Returns true if it matches or if extFilter is empty.
bool HasExtension(const std::wstring& file, const std::wstring& extFilter)
{
    if (extFilter.empty())
        return true; // no filtering applied

    std::wstring lowerFile = ToLower(file);
    std::wstring lowerFilter = ToLower(extFilter);
    size_t pos = lowerFile.rfind(L'.');
    if (pos == std::wstring::npos)
        return false;
    std::wstring fileExt = lowerFile.substr(pos);
    return fileExt == lowerFilter;
}

//------------------------------------------------------------------------------
// EnumerateFilesAndGroupBySize()
//   Recursively enumerates all files under a given directory and, for each file
//   that passes the optional extension filter, extracts its size from the
//   WIN32_FIND_DATA and inserts the file path directly into a sizeGroups map.
// Parameters:
//   directory - The root directory to search.
//   sizeGroups - Out parameter; a hash map where key is file size and value is a
//                vector of file paths of that size.
//   extFilter - Optional file extension filter (e.g., ".txt"). If empty, all files are included.
void EnumerateFilesAndGroupBySize(const std::wstring& directory,
    std::map<ULONGLONG, std::vector<std::wstring>>& sizeGroups,
    const std::wstring& extFilter = L"")
{
    std::wstring searchPath = directory + L"\\*";
    WIN32_FIND_DATA findData;
    HANDLE hFind = FindFirstFile(searchPath.c_str(), &findData);
    if (hFind == INVALID_HANDLE_VALUE)
        return;

    do
    {
        // Exclude links. (They have the REPARSE_POINT flag)
        if (findData.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT)
            continue;

        std::wstring name = findData.cFileName;
        if (name == L"." || name == L"..")
            continue;

        std::wstring fullPath = directory + L"\\" + name;
        if (findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
        {
            // Recurse into the subdirectory.
            EnumerateFilesAndGroupBySize(fullPath, sizeGroups, extFilter);
        }
        else
        {
            if (HasExtension(fullPath, extFilter))
            {
                // Compute file size directly from findData.
                ULONGLONG size = (static_cast<ULONGLONG>(findData.nFileSizeHigh) << 32) |
                    findData.nFileSizeLow;

                if (size < MIN_SIZE_TO_CONSIDER)
                    continue;

                // Insert the file path into the appropriate size bucket.
                sizeGroups[size].push_back(fullPath);
            }
        }
    } while (FindNextFile(hFind, &findData) != 0);

    FindClose(hFind);
}

//------------------------------------------------------------------------------
// GetFileUniqueIdAndLinkCount
//   Opens a file and retrieves its unique identifier and link count using the
//   BY_HANDLE_FILE_INFORMATION structure.
// Parameters:
//   filePath - the full file path
//   uniqueId - (out) unique identifier (constructed from nFileIndexHigh/Low)
//   numLinks - (out) number of hard links for the file
// Returns:
//   true on success, false on failure.
bool GetFileUniqueIdAndLinkCount(const std::wstring& filePath,
    ULONGLONG& uniqueId,
    DWORD& numLinks)
{
    HANDLE hFile = CreateFileW(filePath.c_str(),
        0,  // No need for read/write permission
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,  // Needed for directories, not used here but safe
        nullptr);
    if (hFile == INVALID_HANDLE_VALUE)
    {
        std::wcerr << L"Failed to open file: " << filePath
            << L", error: " << GetLastError() << std::endl;
        return false;
    }

    BY_HANDLE_FILE_INFORMATION fileInfo = { 0 };
    if (!GetFileInformationByHandle(hFile, &fileInfo))
    {
        std::wcerr << L"Failed to get file information for: " << filePath
            << L", error: " << GetLastError() << std::endl;
        CloseHandle(hFile);
        return false;
    }

    CloseHandle(hFile);
    uniqueId = (static_cast<ULONGLONG>(fileInfo.nFileIndexHigh) << 32) | fileInfo.nFileIndexLow;
    numLinks = fileInfo.nNumberOfLinks;
    return true;
}

//------------------------------------------------------------------------------
// DeduplicateGroup
//   Given a vector of file paths that are known duplicates, this routine chooses the first
//   file as the master copy and replaces the other duplicates with hard links to that master,
//   provided they are not already hard links to the master.
//   Returns true if the routine processed the group (errors are logged).
bool DeduplicateGroup(const std::vector<std::wstring>& duplicateGroup)
{
    if (duplicateGroup.size() < 2)
    {
        std::wcerr << L"No duplicates in group to deduplicate." << std::endl;
        return true;
    }

    // Get the unique file id of the master file (first in group).
    const std::wstring master = duplicateGroup[0];
    ULONGLONG masterId = 0;
    DWORD masterLinks = 0;
    if (!GetFileUniqueIdAndLinkCount(master, masterId, masterLinks))
    {
        std::wcerr << L"Failed to get unique ID for master file: " << master << std::endl;
        return false;
    }
    std::wcout << L"Master file: " << master << std::endl;

    // Process each duplicate file (skip the master).
    for (size_t i = 1; i < duplicateGroup.size(); ++i)
    {
        const std::wstring& dupFile = duplicateGroup[i];

        // Get the unique file id for this duplicate.
        ULONGLONG dupId = 0;
        DWORD dupLinks = 0;
        if (!GetFileUniqueIdAndLinkCount(dupFile, dupId, dupLinks))
        {
            std::wcerr << L"Failed to get unique ID for file: " << dupFile << std::endl;
            continue;
        }

        // If this duplicate already shares the same unique id as the master,
        // then it is already a hard link to the master. Skip it.
        if (dupId == masterId)
        {
            std::wcout << L"Skipping file (already linked): " << dupFile << std::endl;
            continue;
        }

        // Delete the duplicate file.
        if (!DeleteFileW(dupFile.c_str()))
        {
            DWORD delErr = GetLastError();
            std::wcerr << L"Error deleting duplicate file: " << dupFile
                << L". Error code: " << delErr << std::endl;
            continue;
        }

        // Create a hard link from the duplicate's path pointing to the master.
        if (!CreateHardLinkW(dupFile.c_str(), master.c_str(), nullptr))
        {
            DWORD hlErr = GetLastError();
            std::wcerr << L"Error creating hard link for: " << dupFile
                << L" pointing to: " << master
                << L". Error code: " << hlErr << std::endl;
            continue;
        }
        else
        {
            std::wcout << L"Replaced duplicate " << dupFile << L" with hard link to " << master << std::endl;
        }
    }
    return true;
}

//------------------------------------------------------------------------------
// main()
//    Entry point: enumerates files from a root folder and optionally filters by extension,
//    groups files by size and then by content using our hash–based three–way partition scheme,
//    and outputs the duplicate file groups.
int wmain(int argc, wchar_t* argv[])
{
    _setmode(_fileno(stdout), _O_U16TEXT);

    if (argc < 2)
    {
        std::wcerr << L"Usage: " << argv[0] << L" <root_folder> [extension_filter]" << std::endl;
        std::wcerr << L"Example: " << argv[0] << L" C:\\MyFolder .txt" << std::endl;
        return 1;
    }

    std::wstring rootFolder = argv[1];
    std::wstring extFilter;
    if (argc >= 3)
    {
        extFilter = argv[2];
    }

    std::map<ULONGLONG, std::vector<std::wstring>> sizeGroups;
    EnumerateFilesAndGroupBySize(rootFolder, sizeGroups, extFilter);

    std::vector<std::vector<std::wstring>> allDuplicateGroups;

    // For each same-size group (excluding groups with only one file) group by content.
    for (const auto& entry : sizeGroups)
    {
        if (entry.second.size() < 2)
            continue;

        std::vector<std::vector<std::wstring>> duplicateGroups;

        GroupFilesByContentUsingMap(entry.second, duplicateGroups, 0);

        // Output the duplicate groups.
        for (const auto& group : duplicateGroups)
        {
            if (group.size() < 2)
                continue;

            allDuplicateGroups.push_back(group);

            std::wcout << L"\nDuplicate Group #" << allDuplicateGroups.size() << L" size " << entry.first << L":\n";
            for (const auto& file : group)
                std::wcout << L"  " << file << std::endl;
        }
    }

    if (allDuplicateGroups.empty())
        std::wcout << L"\nNo duplicate files found." << std::endl;

    //*
    for (const auto& group : allDuplicateGroups)
    {
        std::wcout << L"*";
        if (!DeduplicateGroup(group))
        {
            std::wcerr << L"\nFailed to deduplicate group:\n";
            for (const auto& file : group)
                std::wcerr << L"  " << file << std::endl;

            return 1;
        }
    }
    //*/

    return 0;
}
