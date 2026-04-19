#dfs.py
import hashlib
import heapq
import json
import os
import tempfile
import time

HASH_BITS = 4  # must match Chord's m value

def dfsHash(keyString):
    return int(hashlib.sha1(keyString.encode()).hexdigest(), 16) % (2 ** HASH_BITS)


def sortable_key(s: str):
    """
    Compare keys for global ordering: integers, then floats, else lexicographic string order.
    """
    t = (s or "").strip()
    if not t:
        return (3, "")
    try:
        return (0, int(t))
    except ValueError:
        pass
    try:
        return (1, float(t))
    except ValueError:
        return (2, t)


def parse_kv_line(line: str):
    """Parse a single `key,value` record (first comma). Returns None if empty or invalid."""
    s = line.strip()
    if not s:
        return None
    sep = s.find(",")
    if sep < 0:
        return None
    return s[:sep].strip(), s[sep + 1 :].strip()


def decode_page_blob(page_raw: str) -> str:
    """Decode a stored page value (JSON `{"content": ...}` or legacy plain string)."""
    try:
        page = json.loads(page_raw)
    except Exception:
        return str(page_raw)
    if isinstance(page, dict) and "content" in page:
        return page["content"]
    return str(page_raw)


def validate_sorted_kv_text(text: str) -> None:
    """
    Ensure non-empty parsed records appear in non-decreasing sortable_key order.
    Raises ValueError if validation fails.
    """
    prev = None
    for raw in text.splitlines():
        p = parse_kv_line(raw)
        if p is None:
            continue
        sk = sortable_key(p[0])
        if prev is not None and sk < prev:
            raise ValueError(f"records out of order: {prev!r} then {sk!r}")
        prev = sk

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
            allContent.append(decode_page_blob(pageRaw))

        return "".join(allContent)

    def iter_file_pages(self, fileName):
        """
        Scan the distributed file page-by-page in metadata order (same order as read()).
        Yields (page_no, content) for each stored page.
        """
        raw = self.chordNode.get(self.metaKey(fileName))
        if raw is None:
            raise FileNotFoundError(f"No such DFS file: {fileName}")

        meta = FileMetaData.fromDict(json.loads(raw))

        for pageDesc in meta.pages:
            page_no = pageDesc["pageNo"]
            pageRaw = self.chordNode.get(pageDesc["key"])
            yield page_no, decode_page_blob(pageRaw)

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

    # DFS operations - Part B (distributed sort)

    @staticmethod
    def _assemble_sorted_output(chunks):
        """Merge locally sorted shards into one globally sorted text body (output assembly)."""
        if not chunks:
            return "", []
        streams = [
            ((sortable_key(k), k, v) for k, v in chunk) for chunk in chunks
        ]
        merged = list(heapq.merge(*streams))
        lines_out = [f"{k},{v}" for _, k, v in merged]
        body = "\n".join(lines_out)
        if lines_out:
            body += "\n"
        return body, lines_out

    def sort_file(self, filename, output_filename):
        """
        Distributed sort pipeline:
          1) Page scanning — walk each DFS page in order.
          2) Routing — each key,value record goes to the Chord successor of dfsHash(key).
          3) Local sorted aggregation — each peer sorts its buffer (StorageNode).
          4) Output assembly — k-way merge of shards; validate global order; store in DFS.
        """
        st = self.chordNode
        st.sort_reset_all()

        for _page_no, page_content in self.iter_file_pages(filename):
            for raw in page_content.splitlines():
                parsed = parse_kv_line(raw)
                if parsed is None:
                    continue
                k, v = parsed
                st.route_sort_record(k, v)

        chunks = st.sort_collect_sorted_shards()
        body, _lines_out = self._assemble_sorted_output(chunks)
        validate_sorted_kv_text(body)

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt", encoding="utf-8", newline="\n"
        ) as tf:
            tf.write(body)
            path = tf.name
        try:
            self.touch(output_filename)
            self.append(output_filename, path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass