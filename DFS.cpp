#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
#include <functional>
#include <map>
#include <unistd.h>
#include <set>
#include <thread>
#include <chrono>
#include <mutex>
#include <fstream>
#include <atomic>
#include <sstream>
#include <cstring>
#include <iomanip>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

using namespace std;

// declaring Mutex Lock
mutex hbMutex;

// constants
const int CHUNK_SIZE = 32;
const int REPLICATION_FACTOR = 3;
const int HEARTBEAT_INTERVAL = 1;
const int FAILURE_DETECTION_TIMEOUT = 3;

// MESSAGE TAGS
enum MessageType
{
    UPLOAD_REQUEST = 1,
    CHUNK_DATA = 2,
    DOWNLOAD_REQUEST = 3,
    SEARCH_REQUEST = 4,
    HEARTBEAT = 5,
    FAILOVER = 6,
    RECOVER = 7,
    EXIT_TAG = 999
};

// STRUCT for FILE METADATA
struct FileMetadata
{
    string fileName;
    string fileAddress;
    int numChunks;
    long long int startChunkID;
    map<long long int, set<int>> chunkPositions; // chunkID : replicaNodes
};

// STRUCT to hold SEARCH RESULTS
struct SearchResult
{
    int offset;      // Offset within the chunk
    bool isPartial;  // True if word spans across chunks
    int matchLength; // Length of the match found in current chunk
    bool isPrefix;   // True if this is prefix part of split word
};

// FUNCTIONS DECLARATIONs
void processMetadataServer(int numProcesses);
void processStorageNode(int myRank);

pair<int, string> opUploadMS(long long int &lastChunkID, string fileName, string fileAddress, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes, set<pair<int, int>> &loadPerNode);
pair<int, string> opRetrieveMS(string fileName, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes);
pair<int, string> opSearchMS(string fileName, const string &word, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes);
pair<int, string> opListFileMS(string fileName, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes);

void opUploadSS(map<int, string> &storedChunks);
void opRetrieveSS(map<int, string> &storedChunks);
void opSearchSS(map<int, string> &storedChunks);

void printFileData(string fileData);
pair<int, string> readFileInChunks(const string &fileName, const string &fileAddress, vector<string> &fileChunks);
pair<int, vector<size_t>> findWordOccurrences(const string &chunkContent, int chunkOffset, const string &word);
bool isCompleteMatch(const string &chunk, const string &word, size_t pos);
bool isPrefixMatch(const string &chunk, const string &word, size_t pos, char &prevChunkLastChar);
bool isSuffixMatch(const string &chunk, const string &word, int prevMatchLength);
vector<SearchResult> searchInChunk(const string &chunk, const string &searchWord, const int &chunkOffset, int &prevMatchLength);

void monitorHeartbeats(unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning);
void monitorHeartbeatsWrapper(unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning);

void thread_receiveHB(int nodeRank, unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning);
void thread_receiveHBWrapper(int nodeRank, unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning);

void thread_sendHB(bool &isRunning);
void thread_sendHBWrapper(bool &isRunning);

string convertCharArrToString(char *arr, int arrSize)
{
    string str(arr, arrSize);
    return str;
}

/*




































*/

void monitorHeartbeats(unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning)
{
    while (isRunning)
    {
        this_thread::sleep_for(chrono::milliseconds(500));
        auto now = chrono::steady_clock::now();

        lock_guard<mutex> lock(hbMutex);
        for (auto it = heartbeatTimestamps.begin(); it != heartbeatTimestamps.end();)
        {
            if (chrono::duration_cast<chrono::seconds>(now - it->second).count() > FAILURE_DETECTION_TIMEOUT)
            {
                // cout << "Node " << it->first << " : DOWN!" << endl;
                it = heartbeatTimestamps.erase(it); // Remove the node from the map
            }
            else
            {
                ++it;
            }
        }
    }

    return;
}

void monitorHeartbeatsWrapper(unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning)
{
    monitorHeartbeats(heartbeatTimestamps, isRunning);
}

void thread_receiveHB(int nodeRank, unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning)
{
    while (isRunning)
    {
        // receiving HB if sent
        MPI_Status status;
        int msgPresent;
        MPI_Iprobe(nodeRank, HEARTBEAT, MPI_COMM_WORLD, &msgPresent, &status);

        // checking if message is present
        if (msgPresent)
        {
            // receiving the sent message
            MPI_Recv(nullptr, 0, MPI_BYTE, nodeRank, HEARTBEAT, MPI_COMM_WORLD, &status);

            // updating the timestamp
            lock_guard<mutex> lock(hbMutex);
            heartbeatTimestamps[nodeRank] = chrono::steady_clock::now();
        }
    }

    return;
}

void thread_receiveHBWrapper(int nodeRank, unordered_map<int, chrono::time_point<chrono::steady_clock>> &heartbeatTimestamps, bool &isRunning)
{
    thread_receiveHB(nodeRank, heartbeatTimestamps, isRunning);
}

void thread_sendHB(bool &isRunning)
{
    // sending HB after every 2 seconds
    while (isRunning)
    {
        // sending HB to only Root
        MPI_Send(nullptr, 0, MPI_BYTE, 0, HEARTBEAT, MPI_COMM_WORLD);

        this_thread::sleep_for(chrono::seconds(HEARTBEAT_INTERVAL));
    }
}

void thread_sendHBWrapper(bool &isRunning)
{
    thread_sendHB(isRunning);
}

/*








































*/

// processing metadata server
void processMetadataServer(int numProcesses)
{
    // Heartbeat Storage
    unordered_map<int, chrono::time_point<chrono::steady_clock>> heartbeatTimestamps; // (nodeRank, timestamp)
    bool isRunning = true;

    // Metadata Storage
    map<string, FileMetadata> metadata; // (fileName, FileMetadata)
    unordered_set<int> activeNodes;
    long long int lastChunkID = 0;
    set<pair<int, int>> loadPerNode; // (load, nodeRank)

    // initializing loadPerNode
    for (int i = 1; i < numProcesses; i++)
        loadPerNode.insert({0, i});

    // updating active nodes
    for (int i = 1; i < numProcesses; i++)
    {
        activeNodes.insert(i);
    }

    // creating a thread for monitoring heartbeats
    thread t(monitorHeartbeatsWrapper, ref(heartbeatTimestamps), ref(isRunning));

    // creating a thread for each process
    vector<thread> allThreads;
    for (int i = 1; i < numProcesses; i++)
    {
        allThreads.push_back(thread(thread_receiveHBWrapper, i, ref(heartbeatTimestamps), ref(isRunning)));
    }

    // getting constant input commands
    string command;
    while (isRunning)
    {
        // getting command
        getline(cin, command);

        // processing command
        istringstream iss(command);
        string operation;
        iss >> operation;

        // checking which operation
        if (operation == "exit")
        {
            // if anything else : ERROR
            string temp;
            iss >> temp;
            if (!temp.empty())
            {
                cout << -1 << endl;
                continue;
            }

            // notifying all Storage Servers to terminate
            for (int i = 1; i < numProcesses; i++)
            {
                MPI_Send(nullptr, 0, MPI_BYTE, i, EXIT_TAG, MPI_COMM_WORLD);
            }

            isRunning = false;
            break;
        }
        else if (operation == "upload")
        {
            // getting file name
            string fileName;
            iss >> fileName;

            string fileAddress;
            iss >> fileAddress;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (fileName.empty() || fileAddress.empty() || !temp.empty())
            {
                cout << -1 << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opUploadMS(lastChunkID, fileName, fileAddress, metadata, activeNodes, loadPerNode);
            cout << retVal.first << endl;

            // listing file if uploaded successfully
            if (retVal.first == 1)
            {
                opListFileMS(fileName, metadata, activeNodes);
            }
        }
        else if (operation == "retrieve")
        {
            string fileName;
            iss >> fileName;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (fileName.empty() || !temp.empty())
            {
                cout << -1 << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opRetrieveMS(fileName, metadata, activeNodes);
            if (retVal.first == -1)
                cout << retVal.first << endl;
            else
            {
                printFileData(retVal.second);
            }
        }
        else if (operation == "search")
        {
            // getting file name
            string fileName;
            iss >> fileName;

            string word;
            iss >> word;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (fileName.empty() || word.empty() || !temp.empty())
            {
                cout << -1 << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opSearchMS(fileName, word, metadata, activeNodes);
            if (retVal.first == -1)
            {
                cout << retVal.first << endl;
            }
        }
        else if (operation == "list_file")
        {
            // getting file name
            string fileName;
            iss >> fileName;

            string temp;
            iss >> temp;

            // if no fileName is entered then error
            if (fileName.empty() || !temp.empty())
            {
                cout << -1 << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opListFileMS(fileName, metadata, activeNodes);
            if (retVal.first == -1)
            {
                cout << retVal.first << endl;
            }
        }
        else if (operation == "failover")
        {
            // getting node rank
            int nodeRank;
            iss >> nodeRank;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (!temp.empty())
            {
                cout << -1 << endl;
                continue;
            }
            if (nodeRank <= 0 || nodeRank >= numProcesses)
            {
                cout << -1 << endl;
                continue;
            }
            if (activeNodes.find(nodeRank) == activeNodes.end())
            {
                cout << -1 << endl;
                continue;
            }

            // sending failover command to the node
            MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, FAILOVER, MPI_COMM_WORLD);

            // updating active nodes
            activeNodes.erase(nodeRank);

            cout << 1 << endl;
        }
        else if (operation == "recover")
        {
            // getting node rank
            int nodeRank;
            iss >> nodeRank;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (!temp.empty())
            {
                cout << -1 << endl;
                continue;
            }
            if (nodeRank <= 0 || nodeRank >= numProcesses)
            {
                cout << -1 << endl;
                continue;
            }
            if (activeNodes.find(nodeRank) != activeNodes.end())
            {
                cout << -1 << endl;
                continue;
            }

            // sending recovery command to the node
            MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, RECOVER, MPI_COMM_WORLD);

            // updating active nodes
            activeNodes.insert(nodeRank);

            cout << 1 << endl;
        }
        else
        {
            cout << -1 << endl;
        }
    }

    // joining all threads
    t.join();
    for (int i = 0; i < allThreads.size(); i++)
    {
        allThreads[i].join();
    }

    return;
}

// processing storage node
void processStorageNode(int myRank)
{
    // storage for chunks
    map<int, string> storedChunks;
    bool isRunning = true;

    // creating a thread to send msgs
    thread t(thread_sendHBWrapper, ref(isRunning));

    // looping to receive commands from Metadata Server
    while (isRunning)
    {
        // checking for commands
        MPI_Status status;
        int msgPresent;
        MPI_Iprobe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &msgPresent, &status);

        // checking if message is present
        if (msgPresent)
        {
            // checking which command
            if (status.MPI_TAG == EXIT_TAG)
            {
                // receiving the sent message & exiting
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, EXIT_TAG, MPI_COMM_WORLD, &status);
                isRunning = false;
                break;
            }
            else if (status.MPI_TAG == UPLOAD_REQUEST)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, UPLOAD_REQUEST, MPI_COMM_WORLD, &status);

                // handling the request
                opUploadSS(storedChunks);
            }
            else if (status.MPI_TAG == DOWNLOAD_REQUEST)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, DOWNLOAD_REQUEST, MPI_COMM_WORLD, &status);

                // handling the request
                opRetrieveSS(storedChunks);
            }
            else if (status.MPI_TAG == SEARCH_REQUEST)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, SEARCH_REQUEST, MPI_COMM_WORLD, &status);

                // handling the request
                opSearchSS(storedChunks);
            }
            else if (status.MPI_TAG == FAILOVER)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, FAILOVER, MPI_COMM_WORLD, &status);

                // stopping the thread
                isRunning = false;
                t.join();
                isRunning = true;
            }
            else if (status.MPI_TAG == RECOVER)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, RECOVER, MPI_COMM_WORLD, &status);

                // recovering the thread for HB
                t = thread(thread_sendHBWrapper, ref(isRunning));
            }
            else
            {
                // doing something
                continue;
            }
        }
    }

    // joining the thread before exiting
    if (t.joinable())
        t.join();

    return;
}

/*




































*/

pair<int, string> opUploadMS(long long int &lastChunkID, string fileName, string fileAddress, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes, set<pair<int, int>> &loadPerNode)
{
    // checking if already present
    if (metadata.find(fileName) != metadata.end())
    {
        return {-1, "File already present in the system"};
    }

    // checking if no Nodes are present
    if (activeNodes.empty())
    {
        return {-1, "No active nodes present"};
    }

    // chunking it
    vector<string> fileChunks;
    pair<int, string> retVal = readFileInChunks(fileName, fileAddress, fileChunks);
    if (retVal.first == -1)
        return retVal;

    // updating metadata
    metadata[fileName].fileName = fileName;
    metadata[fileName].fileAddress = fileAddress;
    metadata[fileName].startChunkID = lastChunkID;
    metadata[fileName].numChunks = fileChunks.size();

    // splitting chunks across ACTIVE servers, RR
    for (int i = 0; i < fileChunks.size(); i++)
    {
        // getting chunk
        string chunk = fileChunks[i];

        // getting replica nodes
        int totalReplicaNodes = min(REPLICATION_FACTOR, static_cast<int>(activeNodes.size()));
        set<int> replicaNodes;
        vector<pair<int, int>> loads(totalReplicaNodes);
        pair<int, int> tempPair;
        for (int j = 0; j < totalReplicaNodes; j++)
        {
            // getting first pair from loadPerNode, which is active too & removing it
            for (auto it = loadPerNode.begin(); it != loadPerNode.end(); it++)
            {
                if (activeNodes.find(it->second) != activeNodes.end())
                {
                    tempPair = *it;
                    loadPerNode.erase(it);
                    break;
                }
            }

            // storing
            loads[j] = tempPair;
            replicaNodes.insert(tempPair.second);
        }

        // updating metadata
        metadata[fileName].chunkPositions[lastChunkID] = replicaNodes;

        // sending chunks to replica nodes
        for (int j = 0; j < totalReplicaNodes; j++)
        {
            int nodeRank = loads[j].second;

            // letting the node know that there is upload request
            MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, UPLOAD_REQUEST, MPI_COMM_WORLD);

            // sending chunk ID
            MPI_Send(&lastChunkID, 1, MPI_LONG_LONG, nodeRank, UPLOAD_REQUEST, MPI_COMM_WORLD);

            // sending chunk data
            int chunkSize = chunk.length();
            MPI_Send(&chunkSize, 1, MPI_INT, nodeRank, CHUNK_DATA, MPI_COMM_WORLD);
            MPI_Send(chunk.c_str(), chunkSize, MPI_CHAR, nodeRank, CHUNK_DATA, MPI_COMM_WORLD);
            // MPI_Send(chunk.c_str(), CHUNK_SIZE, MPI_CHAR, nodeRank, CHUNK_DATA, MPI_COMM_WORLD);

            // incrementing load
            loads[j].first++;

            // adding pair back to loadPerNode
            loadPerNode.insert(loads[j]);
        }

        // incrementing last chunk ID
        lastChunkID++;
    }

    return {1, "File uploaded successfully"};
}

void opUploadSS(map<int, string> &storedChunks)
{
    // creating variables
    char buffer[CHUNK_SIZE];
    memset(buffer, 0, CHUNK_SIZE);
    long long int chunkID;

    // receiving chunk ID
    MPI_Recv(&chunkID, 1, MPI_LONG_LONG, 0, UPLOAD_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // receiving chunk data
    int chunkSize;
    MPI_Recv(&chunkSize, 1, MPI_INT, 0, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(buffer, chunkSize, MPI_CHAR, 0, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (chunkSize < CHUNK_SIZE)
        buffer[chunkSize] = '\0';

    // converting buffer to string
    string chunkData = convertCharArrToString(buffer, chunkSize);

    // storing the chunk
    storedChunks[chunkID] = chunkData;

    return;
}

pair<int, string> readFileInChunks(const string &fileName, const string &fileAddress, vector<string> &fileChunks)
{
    // opening the file with relative fileAddress
    ifstream file(fileAddress, ios::binary); // binary mode
    // ifstream file(fileAddress); // text mode
    if (!file.is_open())
    {
        return {-1, "File not found"};
    }

    // reading the file in 32 byte sized chunks
    char buffer[CHUNK_SIZE];
    while (file.read(buffer, sizeof(buffer)))
    {
        // adding full chunk to vector
        fileChunks.emplace_back(buffer, sizeof(buffer));
    }

    // handling the last chunk
    streamsize bytesRead = file.gcount(); // Get the number of bytes read
    if (bytesRead > 0)
    {
        // reading the remaning bytes
        string lastChunk(buffer, bytesRead);

        // // padding with null + `
        // lastChunk += '\0';
        // lastChunk.append(32 - bytesRead - 1, '`');

        // // padding with null
        // lastChunk.append(32 - bytesRead, '\0');

        fileChunks.push_back(lastChunk);
    }

    // closing the file
    file.close();

    return {1, "File read successfully"};
}

/*






































*/

void printFileData(string fileData)
{
    // printing file data
    cout << fileData << endl;

    return;
}

pair<int, string> opRetrieveMS(string fileName, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes)
{
    // checking if file is present or not
    if (metadata.find(fileName) == metadata.end())
    {
        return {-1, "File not found"};
    }

    // getting metadata
    FileMetadata fileMD = metadata[fileName];

    // iterating over all chunks
    string fileData;
    for (int i = 0; i < fileMD.numChunks; i++)
    {
        // getting chunk ID
        long long int chunkID = fileMD.startChunkID + i;

        // iterating over replica nodes
        bool chunkFound = false;
        for (int nodeRank : fileMD.chunkPositions[chunkID])
        {
            // checking if nodeRank is active right now
            if (activeNodes.find(nodeRank) != activeNodes.end())
            {
                // setting chunk as found
                chunkFound = true;

                // letting the node know taht download request has come
                MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, DOWNLOAD_REQUEST, MPI_COMM_WORLD);

                // sending chunk ID
                MPI_Send(&chunkID, 1, MPI_LONG_LONG, nodeRank, DOWNLOAD_REQUEST, MPI_COMM_WORLD);

                // receiving chunk data
                char buffer[CHUNK_SIZE];
                memset(buffer, 0, CHUNK_SIZE);
                int chunkSize;
                MPI_Recv(&chunkSize, 1, MPI_INT, nodeRank, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(buffer, chunkSize, MPI_CHAR, nodeRank, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (chunkSize < CHUNK_SIZE)
                    buffer[chunkSize] = '\0';
                // MPI_Recv(buffer, CHUNK_SIZE, MPI_CHAR, nodeRank, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // converting buffer to string
                string chunkData = convertCharArrToString(buffer, chunkSize);

                // adding to file data
                fileData.append(chunkData);

                break;
            }
        }

        // checking if chunk was found
        if (!chunkFound)
        {
            return {-1, "File retrieval failed. No active replica found"};
        }
    }

    // // reducing all useless characters from the end of the file
    // while (fileData.back() == '`')
    // {
    //     fileData.pop_back();
    // }
    // while (fileData.back() == '\0')
    // {
    //     fileData.pop_back();
    // }

    // // counting number of padded null characters at the end
    // int count = 0;
    // for (int i = fileData.length() - 1; i >= 0; i--)
    // {
    //     if (fileData[i] == '\0')
    //     {
    //         count++;
    //     }
    //     else
    //     {
    //         break;
    //     }
    // }
    // cout << "Null Count " << count << endl;

    // // printing last character of the file
    // if (fileData[fileData.length() - 1] == '\0')
    // {
    //     cout << "Its a null character" << endl;
    // }
    // else if (fileData[fileData.length() - 1] == '\n')
    // {
    //     cout << "Its a new line character" << endl;
    // }
    // else if (fileData[fileData.length() - 1] == ' ')
    // {
    //     cout << "Its a space character" << endl;
    // }
    // else
    // {
    //     cout << "Its a valid character" << endl;
    // }

    return {1, fileData};
}

void opRetrieveSS(map<int, string> &storedChunks)
{
    // receiving chunkID
    long long int chunkID;
    MPI_Recv(&chunkID, 1, MPI_LONG_LONG, 0, DOWNLOAD_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // sending chunk data
    int chunkSize = storedChunks[chunkID].length();
    if (chunkSize == CHUNK_SIZE + 1)
        chunkSize--;
    MPI_Send(&chunkSize, 1, MPI_INT, 0, CHUNK_DATA, MPI_COMM_WORLD);
    MPI_Send(storedChunks[chunkID].c_str(), chunkSize, MPI_CHAR, 0, CHUNK_DATA, MPI_COMM_WORLD);
    // MPI_Send(storedChunks[chunkID].c_str(), CHUNK_SIZE, MPI_CHAR, 0, CHUNK_DATA, MPI_COMM_WORLD);
}

/*






































*/

bool isCompleteMatch(const string &chunk, const string &word, size_t pos, char &prevChunkLastChar)
{
    if (pos + word.length() >= chunk.length())
        return false;

    // forming words
    string temp1{""};
    for (size_t i = 0; i < word.length(); i++)
    {
        // checking if word ends
        if (chunk[pos + i] == '\0' || chunk[pos + i] == '\n' || chunk[pos + i] == ' ' || chunk[pos + i] == '\t')
        {
            break;
        }
        temp1 += chunk[pos + i];
    }

    // also making sure that the word begins exactly from pos
    if (pos > 0 && chunk[pos - 1] != '\0' && chunk[pos - 1] != '\n' && chunk[pos - 1] != ' ' && chunk[pos - 1] != '\t')
    {
        return false;
    }
    else if (pos == 0)
    {
        // checking with last character of the previous chunk
        if (prevChunkLastChar != '\0' && prevChunkLastChar != '\n' && prevChunkLastChar != ' ' && prevChunkLastChar != '\t')
        {
            return false;
        }
    }

    // also making sure that the word is ending after it
    if ((word.length() < CHUNK_SIZE) && chunk[pos + word.length()] != '\0' && chunk[pos + word.length()] != '\n' && chunk[pos + word.length()] != ' ' && chunk[pos + word.length()] != '\t')
    {
        return false;
    }

    temp1 += '\0';
    string temp2 = word.substr(0, word.length());
    temp2 += '\0';

    // comparing both
    if (temp1 == temp2)
    {
        return true;
    }

    return false;
}

bool isPrefixMatch(const string &chunk, const string &word, size_t pos, char &prevChunkLastChar)
{
    if (pos >= chunk.length())
        return false;

    int chunkSize = chunk.length();
    if (chunkSize == CHUNK_SIZE + 1)
        chunkSize--;
    size_t remainingChars = chunkSize - pos;

    // checking if words starts from correct position as a separate word
    if (pos > 0 && chunk[pos - 1] != '\0' && chunk[pos - 1] != '\n' && chunk[pos - 1] != ' ' && chunk[pos - 1] != '\t')
    {
        return false;
    }
    else if (pos == 0 && prevChunkLastChar != '\0' && prevChunkLastChar != '\n' && prevChunkLastChar != ' ' && prevChunkLastChar != '\t')
    {
        return false;
    }

    string temp1 = chunk.substr(pos, remainingChars);
    temp1 += '\0';
    string temp2 = word.substr(0, remainingChars);
    temp2 += '\0';
    if (temp1 == temp2)
    {
        return true;
    }
    return false;
}

bool isSuffixMatch(const string &chunk, const string &word, int prevMatchLength)
{
    if (prevMatchLength > word.length())
        return false;
    size_t remainingLength = word.length() - prevMatchLength;
    if (remainingLength > chunk.length())
        return false;
    if (remainingLength == 0)
    {
        // check if the first character of chunk is a valid character
        if (chunk[0] == '\0' || chunk[0] == '\n' || chunk[0] == ' ' || chunk[0] == '\t')
        {
            return true;
        }
        return false;
    }

    // checking if word ends in file after suffix length
    if (chunk[remainingLength] != '\0' && chunk[remainingLength] != '\n' && chunk[remainingLength] != ' ' && chunk[remainingLength] != '\t')
    {
        return false;
    }

    string temp1 = chunk.substr(0, remainingLength);
    temp1 += '\0';
    string temp2 = word.substr(prevMatchLength, remainingLength);
    temp2 += '\0';

    if (temp1 == temp2)
    {
        return true;
    }
    return false;
}

vector<SearchResult> searchInChunk(const string &chunk, const string &searchWord, const int &chunkOffset, int &prevMatchLength, char &prevChunkLastChar)
{
    vector<SearchResult> results;

    // handling empty inputs
    if (chunk.empty() || searchWord.empty())
    {
        return results;
    }

    // checking for continuation from the previous chunk
    if (prevMatchLength > 0)
    {
        if (isSuffixMatch(chunk, searchWord, prevMatchLength))
        {
            SearchResult result = {
                chunkOffset - prevMatchLength,
                true,
                static_cast<int>(searchWord.length() - prevMatchLength),
                false // since it is the suffix part
            };

            results.push_back(result);
        }
    }

    // searching for full matches & potential across chunk matches
    int chunkSize = chunk.length();
    if (chunkSize == CHUNK_SIZE + 1)
        chunkSize--;
    for (size_t i = 0; i < chunkSize; i++)
    {
        // checking for full match
        if (isCompleteMatch(chunk, searchWord, i, prevChunkLastChar))
        {
            SearchResult result = {
                chunkOffset + static_cast<int>(i),
                false, // since not a partial match
                static_cast<int>(searchWord.length()),
                false};

            results.push_back(result);
        }
        else if (isPrefixMatch(chunk, searchWord, i, prevChunkLastChar))
        {
            // partial match at boundary as prefix
            SearchResult result = {
                chunkOffset + static_cast<int>(i),
                true,
                static_cast<int>(chunkSize - i),
                true // it is the prefix part
            };

            results.push_back(result);
        }
    }

    return results;
}

pair<int, string> opSearchMS(string fileName, const string &word, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes)
{
    // checking if file is present
    if (metadata.find(fileName) == metadata.end())
    {
        return {-1, "File not found"};
    }

    // getting file metadata
    FileMetadata fileMD = metadata[fileName];

    // iterating over all chunks
    vector<int> allOffsets;
    int prevMatchLength = 0;
    char prevChunkLastChar = '\0';
    for (int i = 0; i < fileMD.numChunks; i++)
    {
        // getting chunk ID & firstOffset
        long long int chunkID = fileMD.startChunkID + i;
        int chunkOffset = i * CHUNK_SIZE;

        // iterating over replica nodes
        bool chunkFound = false;
        for (int nodeRank : fileMD.chunkPositions[chunkID])
        {
            // checking if nodeRank is active right now
            if (activeNodes.find(nodeRank) != activeNodes.end())
            {
                // setting chunk as found
                chunkFound = true;

                // letting the node know that search request has come
                MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);

                // sending chunk ID
                MPI_Send(&chunkID, 1, MPI_LONG_LONG, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);

                // sending word size and word
                int wordSize = word.length();
                MPI_Send(&wordSize, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);
                MPI_Send(word.c_str(), word.length(), MPI_CHAR, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);

                // sending chunk offset
                MPI_Send(&chunkOffset, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);

                // sending previous match length
                MPI_Send(&prevMatchLength, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);

                // sending previous chunk last character
                MPI_Send(&prevChunkLastChar, 1, MPI_CHAR, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD);

                // resetting prevMatchLength & prevChunkLastChar for this chunk
                prevMatchLength = 0;
                prevChunkLastChar = '\0';

                // receiving last char of chunk
                MPI_Recv(&prevChunkLastChar, 1, MPI_CHAR, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // receiving size of search results
                int resultSize;
                MPI_Recv(&resultSize, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // receiving all search results
                for (int j = 0; j < resultSize; j++)
                {
                    // receiving offset of the chunk
                    int offset;
                    MPI_Recv(&offset, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    // receiving if its partial or not
                    int isPartial;
                    MPI_Recv(&isPartial, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    // checking if partial then
                    if (isPartial)
                    {
                        // receiving if prefix or not
                        int isPrefix;
                        MPI_Recv(&isPrefix, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                        // checking if prefix then
                        if (isPrefix)
                        {
                            // receiving partial match length
                            int matchLength;
                            MPI_Recv(&matchLength, 1, MPI_INT, nodeRank, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                            // updating prevMatchLength
                            prevMatchLength = matchLength;

                            // checking if its the last chunk
                            if (i == fileMD.numChunks - 1 && prevMatchLength == word.length())
                            {
                                // adding the offset
                                allOffsets.push_back(offset);
                            }
                        }
                        else // SUFFIX case
                        {
                            // adding offset to allOffsets
                            allOffsets.push_back(offset);
                        }
                    }
                    else
                    {
                        // adding offset to allOffsets
                        allOffsets.push_back(offset);
                    }
                }

                break;
            }
        }

        // checking if chunk was found
        if (!chunkFound)
        {
            return {-1, "File retrieval failed. No active replica found"};
        }
    }

    // printing out
    cout << allOffsets.size() << endl;
    for (int offset : allOffsets)
    {
        cout << offset << " ";
    }
    cout << endl;

    return {1, "Word search successful"};
}

void opSearchSS(map<int, string> &storedChunks)
{
    // receiving chunkID
    long long int chunkID;
    MPI_Recv(&chunkID, 1, MPI_LONG_LONG, 0, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // receiving word size and word
    int wordSize;
    MPI_Recv(&wordSize, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    char word[wordSize + 1];
    MPI_Recv(word, wordSize, MPI_CHAR, 0, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    word[wordSize] = '\0';

    // receiving offset
    int chunkOffset;
    MPI_Recv(&chunkOffset, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // receiving previous match length & previous chunk last character
    int prevMatchLength;
    char prevChunkLastChar;
    MPI_Recv(&prevMatchLength, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&prevChunkLastChar, 1, MPI_CHAR, 0, SEARCH_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // getting data of the chunk
    string chunkContent = storedChunks[chunkID];

    // handling the search
    vector<SearchResult> results = searchInChunk(chunkContent, word, chunkOffset, prevMatchLength, prevChunkLastChar);

    // sending last char of chunk
    int chunkSize = chunkContent.length();
    if (chunkSize == CHUNK_SIZE + 1)
        chunkSize--;
    char lastChar = chunkContent[chunkSize - 1];
    // char lastChar = chunkContent[CHUNK_SIZE - 1];
    MPI_Send(&lastChar, 1, MPI_CHAR, 0, SEARCH_REQUEST, MPI_COMM_WORLD);

    // sending size of search results
    int resultSize = results.size();
    MPI_Send(&resultSize, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);

    // sending all search results
    for (int i = 0; i < resultSize; i++)
    {
        // sending offset of the chunk
        MPI_Send(&results[i].offset, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);

        // sending if its partial or not
        if (results[i].isPartial)
        {
            int temp = 1;
            MPI_Send(&temp, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);
        }
        else
        {
            int temp = 0;
            MPI_Send(&temp, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);
        }

        // checking if partial then
        if (results[i].isPartial)
        {
            // sending if its prefix or not
            if (results[i].isPrefix)
            {
                int temp = 1;
                MPI_Send(&temp, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);

                // sending partial match length
                MPI_Send(&results[i].matchLength, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);
            }
            else // SUFFIX case
            {
                int temp = 0;
                MPI_Send(&temp, 1, MPI_INT, 0, SEARCH_REQUEST, MPI_COMM_WORLD);
            }
        }
    }

    return;
}

/*






































*/

pair<int, string> opListFileMS(string fileName, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes)
{
    // checking if file is present or not
    if (metadata.find(fileName) == metadata.end())
    {
        return {-1, "File not found"};
    }

    // getting metadata
    FileMetadata fileMD = metadata[fileName];
    long long int startChunkID = fileMD.startChunkID;

    // iterating over all chunks
    for (int i = 0; i < fileMD.numChunks; i++)
    {
        // getting actively stored chunk nodes
        vector<int> chunkActiveNodes;
        for (int nodeRank : fileMD.chunkPositions[startChunkID + i])
        {
            if (activeNodes.find(nodeRank) != activeNodes.end())
            {
                chunkActiveNodes.push_back(nodeRank);
            }
        }

        // printing output
        cout << i << " " << chunkActiveNodes.size() << " ";
        for (int nodeRank : chunkActiveNodes)
        {
            cout << nodeRank << " ";
        }

        cout << endl;
    }

    return {1, "File listed successfully"};
}

/*





































*/

// MAIN
signed main(int argc, char *argv[])
{
    // // redirecting all the output
    // ofstream outFile("out.txt", ios::out);
    // if (!outFile)
    // {
    //     cerr << "Error opening file!" << endl;
    //     return 1;
    // }
    // streambuf *defaultCoutBuffer = cout.rdbuf(outFile.rdbuf());

    // MPI Start
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE)
    {
        cout << "Warning: MPI implementation does not support MPI_THREAD_MULTIPLE";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // basic info
    int myRank, numProcesses;
    MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
    MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);

    if (myRank == 0)
    {
        processMetadataServer(numProcesses);
    }
    else
    {
        processStorageNode(myRank);
    }

    // MPI End
    MPI_Finalize();

    // // redirecting all output back to std out
    // cout.rdbuf(defaultCoutBuffer);
    // outFile.close();

    return 0;
}
