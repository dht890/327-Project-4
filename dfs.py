#dfs.py
import hashlib
import json
import time

HASH_BITS = 4  # must match Chord's m value

def dfsHash(keyString):
    return int(hashlib.sha1(keyString.encode()).hexdigest(), 16) % (2 ** HASH_BITS)

#Oanh Tran Part A
#-------------
#Metadata objects : required file name, total number of pages, file size, ordered list of page numbers and their hashes, version number
class FileMetaData:
    def __init__(self, fileName):
        self.fileName = fileName
        self.byteSize = 0
        self.numPages = 0
        self.pages = []      # ordered list of {"pageNo": int, "key": int}
        self.version = 0

    def toDict(self):
        return self.__dict__

    @staticmethod
    def fromDict(data):
        meta = FileMetaData(data["fileName"])
        meta.byteSize = data["byteSize"]
        meta.numPages = data["numPages"]
        meta.pages = data["pages"]
        meta.version = data["version"]
        return meta

class DFSClient:
    def __init__(self, chordNode):
        #chordNode will be any node in the chord ring which will be the entry point
        self.chordNode = chordNode
        self.metaCache = {}  # optional cache for metadata, can be used to speed up repeated access to same file


    #metadata placement
    def metaKey(self, fileName):
        return dfsHash("metadata:" + fileName)


    #page placement - key must be routed to proper chord successor
    def pageKey(self, fileName, pageNo):
        return dfsHash(fileName + ":" + str(pageNo))


    #key in Chord ring that will store list of all files in DFS
    def dirKey(self):
        return dfsHash("directory:root")


    #DFS operations - Part A

    #creates an empty file with the given name and initializes its metadata
    def touch(self, fileName):
        meta = FileMetaData(fileName)

        t0 = time.perf_counter()
        self.chordNode.put(self.metaKey(fileName), json.dumps(meta.toDict()))
        print("PUT meta:", time.perf_counter() - t0)

        t1 = time.perf_counter()
        dirData = self.chordNode.get(self.dirKey())
        print("GET dir:", time.perf_counter() - t1)

        fileList = json.loads(dirData) if dirData else []

        if fileName not in fileList:
            fileList.append(fileName)

        t2 = time.perf_counter()
        self.chordNode.put(self.dirKey(), json.dumps(fileList))
        print("PUT dir:", time.perf_counter() - t2)

    #adds page to existing file by fetching the current metadata from the chord and reads from local file disk
    def append(self, fileName, localPath):
        metaKey = self.metaKey(fileName)

        # --- Read file locally ---
        with open(localPath, "r") as f:
            content = f.read()

        # --- Compute page key (pageNo will be assigned remotely) ---
        # We can still estimate using hash of content or temp index
        # but simplest: just use next index guess (safe enough here)
        
        # Optional: fetch once for pageNo (still 1 GET total)
        raw = self.chordNode.get(metaKey)
        if raw is None:
            raise FileNotFoundError(f"No such DFS file: {fileName}")

        meta = json.loads(raw)
        pageNo = meta["numPages"]
        pageKey = self.pageKey(fileName, pageNo)

        # --- SINGLE RPC ---
        self.chordNode.append(metaKey, pageKey, content)


    #retrieves the entire file content
    def read(self, fileName):
        raw = self.chordNode.get(self.metaKey(fileName))
        if raw is None:
            raise FileNotFoundError(f"No such DFS file: {fileName}")

        meta = FileMetaData.fromDict(json.loads(raw))

        allContent = []

        for pageDesc in meta.pages:
            pageRaw = self.chordNode.get(pageDesc["key"])

            try:
                page = json.loads(pageRaw)
            except Exception:
                # fallback: old format (plain string)
                allContent.append(str(pageRaw))
                continue

            # safe extraction
            if isinstance(page, dict) and "content" in page:
                allContent.append(page["content"])
            else:
                # fallback for corrupted or old schema
                allContent.append(str(pageRaw))

        return "".join(allContent)

    #returns the first n lines of a file
    def head(self, fileName, n):
        content = self.read(fileName)
        lines = content.splitlines()
        return "\n".join(lines[:n])


    #returns the last n lines of a file
    def tail(self, fileName, n):
        content = self.read(fileName)
        lines = content.splitlines()
        return "\n".join(lines[-n:])


    #removes the file by metadata, looping through every page descriptor and deletes every page from chord,
    #metadata and filename from directory listing
    def deleteFile(self, fileName):
        raw = self.chordNode.get(self.metaKey(fileName))
        if raw is None:
            return
        meta = FileMetaData.fromDict(json.loads(raw))
        for pageDesc in meta.pages:
            self.chordNode.delete(pageDesc["key"])
        self.chordNode.delete(self.metaKey(fileName))
        dirData = self.chordNode.get(self.dirKey())
        fileList = json.loads(dirData) if dirData else []
        if fileName in fileList:
            fileList.remove(fileName)
        self.chordNode.put(self.dirKey(), json.dumps(fileList))


    #list all files in DFS
    def ls(self):
        dirData = self.chordNode.get(self.dirKey())
        return json.loads(dirData) if dirData else []


    #file information without reading content, just metadata
    def stat(self, fileName):
        raw = self.chordNode.get(self.metaKey(fileName))
        if raw is None:
            raise FileNotFoundError(f"No such DFS file: {fileName}")
        meta = FileMetaData.fromDict(json.loads(raw))
        return {
            "fileName": meta.fileName,
            "byteSize": meta.byteSize,
            "numPages": meta.numPages,
            "pages": meta.pages,
            "version": meta.version,
            "metadataKey": self.metaKey(fileName),
        }