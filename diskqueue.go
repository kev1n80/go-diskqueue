package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"regexp"
	"sync"
	"syscall"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG               = LogLevel(1)
	INFO                = LogLevel(2)
	WARN                = LogLevel(3)
	ERROR               = LogLevel(4)
	FATAL               = LogLevel(5)
	numFileMsgBytes     = 8
	maxMetaDataFileSize = 56
)

var badFileNameRegexp, fileNameRegexp *regexp.Regexp

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
	TotalBytesFolderSize() int64
	setRetentionTime(time.Duration)         // this is used for testing only
	getCreateTime(int64) (time.Time, error) // this is used for testing only
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos            int64
	writePos           int64
	readFileNum        int64
	writeFileNum       int64
	readMessages       int64 // Number of read messages. It's used to update depth.
	writeMessages      int64 // Number of write messages. It's used to update depth.
	totalDiskSpaceUsed int64
	depth              int64

	sync.RWMutex

	// instantiation time metadata
	name                string
	dataPath            string
	maxBytesDiskSpace   int64
	retentionTime       time.Duration
	maxBytesPerFile     int64 // cannot change once created
	maxBytesPerFileRead int64
	minMsgSize          int32
	maxMsgSize          int32
	syncEvery           int64         // number of writes per fsync
	syncTimeout         time.Duration // duration of time per fsync
	exitFlag            int32
	needSync            bool

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	depthChan         chan int64
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	logf AppLogFunc

	// disk limit implementation flag
	enableDiskLimitation bool
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {

	return NewWithDiskSpace(name, dataPath,
		0, time.Second*0, maxBytesPerFile,
		minMsgSize, maxMsgSize,
		syncEvery, syncTimeout, logf)
}

// Another constructor that allows users to use Disk Space Limit feature
// If user is not using Disk Space Limit feature, maxBytesDiskSpace will
// be 0
func NewWithDiskSpace(name string, dataPath string,
	maxBytesDiskSpace int64, retentionTime time.Duration, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	enableDiskLimitation := true
	if maxBytesDiskSpace <= 0 {
		enableDiskLimitation = false
	}
	d := diskQueue{
		name:                 name,
		dataPath:             dataPath,
		maxBytesDiskSpace:    maxBytesDiskSpace,
		retentionTime:        retentionTime,
		maxBytesPerFile:      maxBytesPerFile,
		minMsgSize:           minMsgSize,
		maxMsgSize:           maxMsgSize,
		readChan:             make(chan []byte),
		depthChan:            make(chan int64),
		writeChan:            make(chan []byte),
		writeResponseChan:    make(chan error),
		emptyChan:            make(chan int),
		emptyResponseChan:    make(chan error),
		exitChan:             make(chan int),
		exitSyncChan:         make(chan int),
		syncEvery:            syncEvery,
		syncTimeout:          syncTimeout,
		logf:                 logf,
		enableDiskLimitation: enableDiskLimitation,
	}

	err := d.start()
	if err != nil {
		return nil
	}

	return &d
}

// Get the last known state of DiskQueue from metadata and start ioLoop
func (d *diskQueue) start() error {
	// ensure that DiskQueue has enough space to write the metadata file + at least one data file with max size + message size
	if d.enableDiskLimitation && (d.maxBytesDiskSpace <= maxMetaDataFileSize+d.maxBytesPerFile) {
		errorMsg := fmt.Sprintf(
			"disk size limit too small(%d): not enough space for MetaData file (size=%d) and at least one data file with max size (maxBytesPerFile=%d).",
			d.maxBytesDiskSpace, maxMetaDataFileSize, d.maxBytesPerFile)
		d.logf(ERROR, "DISKQUEUE(%s) - %s", errorMsg)
		return errors.New(errorMsg)
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	fileNameRegexp = regexp.MustCompile(`^` + d.name + `.diskqueue.\d+.dat$`)
	badFileNameRegexp = regexp.MustCompile(`^` + d.name + `.diskqueue.\d+.dat.bad$`)

	d.updateTotalDiskSpaceUsed()

	go d.ioLoop()

	return nil
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	depth, ok := <-d.depthChan
	if !ok {
		// ioLoop exited
		depth = d.depth
	}
	return depth
}

// Returns to total size of the contents (files) in the directory located in the dataPath
func (d *diskQueue) TotalBytesFolderSize() int64 {
	var totalFolderSizeBytes int64

	getTotalFolderSizeBytes := func(fileInfo os.FileInfo) error {
		totalFolderSizeBytes += fileInfo.Size()
		return nil
	}

	d.walkDiskQueueDir(getTotalFolderSizeBytes)

	return totalFolderSizeBytes
}

// ReadChan returns the receive-only []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	close(d.depthChan)

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.depth = 0

	if d.enableDiskLimitation {
		d.totalDiskSpaceUsed = 0
		d.readMessages = 0
		d.writeMessages = 0
	}

	return err
}

func (d *diskQueue) getCreateTime(readFileNum int64) (time.Time, error) {
	info, err := os.Stat(d.fileName(readFileNum))
	if err != nil {
		return time.Now(), err
	}
	stat := info.Sys().(*syscall.Stat_t)
	createTime := time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec))
	return createTime, nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		// for "complete" files (i.e. not the "current" file), maxBytesPerFileRead
		// should be initialized to the file's size, or default to maxBytesPerFile
		d.maxBytesPerFileRead = d.maxBytesPerFile
		if d.readFileNum < d.writeFileNum {
			stat, err := d.readFile.Stat()
			if err == nil {
				d.maxBytesPerFileRead = stat.Size()
				if d.enableDiskLimitation {
					// last 8 bytes are reserved for the number of messages in this file
					d.maxBytesPerFileRead -= numFileMsgBytes
				}
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// we only consider rotating if we're reading a "complete" file
	// and since we cannot know the size at which it was rotated, we
	// rely on maxBytesPerFileRead rather than maxBytesPerFile
	if d.readFileNum < d.writeFileNum && d.nextReadPos >= d.maxBytesPerFileRead {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

func (d *diskQueue) removeBadFile(oldestBadFileInfo os.FileInfo) error {
	var err error
	badFileFilePath := path.Join(d.dataPath, oldestBadFileInfo.Name())

	// remove file if it exists
	err = os.Remove(badFileFilePath)
	if err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove .bad file(%s) - %s", d.name, oldestBadFileInfo.Name(), err)
		d.updateTotalDiskSpaceUsed()
		return err
	} else {
		// recaclulate total bad files disk size to get most accurate info
		d.totalDiskSpaceUsed -= oldestBadFileInfo.Size()
		d.logf(INFO, "DISKQUEUE(%s) removed .bad file(%s) of size(%d bytes) to free up disk space", d.name, oldestBadFileInfo.Name(), oldestBadFileInfo.Size())
	}

	return nil
}

func (d *diskQueue) readNumOfMessages(fileName string) (int64, error) {
	var err error

	if d.readFile == nil {
		d.readFile, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
		if err != nil {
			return 0, err
		}
	}

	closeReadFile := func() {
		d.readFile.Close()
		d.readFile = nil
	}
	defer closeReadFile()

	// read total messages number at the end of the file
	_, err = d.readFile.Seek(-numFileMsgBytes, 2)
	if err != nil {
		return 0, err
	}

	var totalMessages int64
	err = binary.Read(d.readFile, binary.BigEndian, &totalMessages)
	if err != nil {
		return 0, err
	}

	return totalMessages, nil
}

func (d *diskQueue) removeReadFile() error {
	if d.readFileNum == d.writeFileNum {
		d.skipToNextRWFile()
		return nil
	}

	curFileName := d.fileName(d.readFileNum)
	totalMessages, err := d.readNumOfMessages(curFileName)
	if err != nil {
		return err
	}

	// update depth with the remaining number of messages
	d.depth -= totalMessages - d.readMessages

	// we have not finished reading this file
	if d.readFileNum == d.nextReadFileNum {
		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	d.moveToNextReadFile()

	return nil
}

// walk through all of the files in the DiskQueue directory
func (d *diskQueue) walkDiskQueueDir(fn func(os.FileInfo) error) error {
	fileInfos, err := ioutil.ReadDir(d.dataPath)

	if err != nil {
		return err
	}

	for _, fileInfo := range fileInfos {
		// only go through files and skip directories
		if !fileInfo.IsDir() {
			err = fn(fileInfo)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *diskQueue) getAllBadFileInfo() ([]os.FileInfo, error) {
	var badFileInfos []os.FileInfo

	getAllBadFileInfo := func(fileInfo os.FileInfo) error {
		// only accept "bad" files created by this DiskQueue object
		if badFileNameRegexp.MatchString(fileInfo.Name()) {
			badFileInfos = append(badFileInfos, fileInfo)
		}

		return nil
	}

	err := d.walkDiskQueueDir(getAllBadFileInfo)

	return badFileInfos, err
}

// get the accurate total non-"bad" file size
func (d *diskQueue) updateTotalDiskSpaceUsed() {
	d.totalDiskSpaceUsed = maxMetaDataFileSize

	updateTotalDiskSpaceUsed := func(fileInfo os.FileInfo) error {
		// only accept files created by this DiskQueue object
		if fileNameRegexp.MatchString(fileInfo.Name()) || badFileNameRegexp.MatchString(fileInfo.Name()) {
			d.totalDiskSpaceUsed += fileInfo.Size()
		}

		return nil
	}

	err := d.walkDiskQueueDir(updateTotalDiskSpaceUsed)
	if err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to update write bytes - %s", d.name, err)
	}
}

func (d *diskQueue) freeDiskSpace(expectedBytesIncrease int64) error {
	var err error
	var badFileInfos []os.FileInfo

	badFileInfos, err = d.getAllBadFileInfo()
	if err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieve all .bad file info - %s", d.name, err)
	}

	// keep freeing up disk space until we have enough space to write this message
	for _, badFileInfo := range badFileInfos {
		if d.totalDiskSpaceUsed+expectedBytesIncrease <= d.maxBytesDiskSpace {
			return nil
		}
		d.removeBadFile(badFileInfo)
	}
	for d.readFileNum <= d.writeFileNum {
		outdated := false
		createTime, err := d.getCreateTime(d.readFileNum)
		if err == nil {
			if time.Since(createTime) > d.retentionTime {
				outdated = true
			}
		}
		if d.totalDiskSpaceUsed+expectedBytesIncrease <= d.maxBytesDiskSpace && !outdated {
			return nil
		}
		// delete the read file (make space)
		readFileToDeleteNum := d.readFileNum
		err = d.removeReadFile()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove file(%s) - %s", d.name, d.fileName(readFileToDeleteNum), err)
			d.handleReadError()
			return err
		} else {
			d.logf(INFO, "DISKQUEUE(%s) removed file(%s) to free up disk space", d.name, d.fileName(readFileToDeleteNum))
		}
		d.updateTotalDiskSpaceUsed()
	}

	if d.totalDiskSpaceUsed+expectedBytesIncrease > d.maxBytesDiskSpace {
		return fmt.Errorf("could not make space for totalDiskSpaceUsed = %d, expectedBytesIncrease = %d, with maxBytesDiskSpace = %d ", d.totalDiskSpaceUsed, expectedBytesIncrease, d.maxBytesDiskSpace)
	}

	return nil
}

// check if there is enough available disk space to write new data to file
func (d *diskQueue) checkDiskSpace(expectedBytesIncrease int64) error {
	// If the data to be written is bigger than the disk size limit, do not write
	if expectedBytesIncrease > d.maxBytesDiskSpace {
		errorMsg := fmt.Sprintf(
			"message size(%d) surpasses disk size limit(%d)",
			expectedBytesIncrease, d.maxBytesDiskSpace)
		d.logf(ERROR, "DISKQUEUE(%s) - %s", d.name, errorMsg)
		return errors.New(errorMsg)
	}

	// check if we have enough space to write this message
	if d.totalDiskSpaceUsed+expectedBytesIncrease > d.maxBytesDiskSpace {
		return d.freeDiskSpace(expectedBytesIncrease)
	}
	return nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) minMsgSize=%d maxMsgSize=%d", dataLen, d.minMsgSize, d.maxMsgSize)
	}

	totalBytes := int64(4 + dataLen)
	reachedFileSizeLimit := false

	if d.enableDiskLimitation {
		expectedBytesIncrease := totalBytes
		// check if we will reach or surpass file size limit
		if d.writePos+totalBytes+numFileMsgBytes >= d.maxBytesPerFile {
			reachedFileSizeLimit = true
			expectedBytesIncrease += numFileMsgBytes
		}

		// free disk space if needed
		err = d.checkDiskSpace(expectedBytesIncrease)
		if err != nil {
			return err
		}
	} else if d.writePos+totalBytes >= d.maxBytesPerFile {
		reachedFileSizeLimit = true
	}

	// add all data to writeBuf before writing to file
	// this causes everything to be written to file or nothing
	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// check if we reached the file size limit with this message
	if d.enableDiskLimitation && reachedFileSizeLimit {
		// write number of messages in binary to file
		err = binary.Write(&d.writeBuf, binary.BigEndian, d.writeMessages+1)
		if err != nil {
			return err
		}
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	d.writePos += totalBytes
	d.depth += 1

	if d.enableDiskLimitation {
		d.totalDiskSpaceUsed += totalBytes
		d.writeMessages += 1
	}

	if reachedFileSizeLimit {
		if d.readFileNum == d.writeFileNum {
			d.maxBytesPerFileRead = d.writePos
		}

		d.writeFileNum++
		d.writePos = 0

		if d.enableDiskLimitation {
			d.writeMessages = 0
		}

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	if d.enableDiskLimitation {
		d.updateTotalDiskSpaceUsed()
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	// if user is using disk space limit feature
	if d.enableDiskLimitation {
		_, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n%d,%d,%d\n",
			&depth,
			&d.readFileNum, &d.readMessages, &d.readPos,
			&d.writeFileNum, &d.writeMessages, &d.writePos)
	} else {
		_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
			&depth,
			&d.readFileNum, &d.readPos,
			&d.writeFileNum, &d.writePos)
	}

	if err != nil {
		return err
	}

	d.depth = depth
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// if user is using disk space limit feature
	if d.enableDiskLimitation {
		_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n%d,%d,%d\n",
			d.depth,
			d.readFileNum, d.readMessages, d.readPos,
			d.writeFileNum, d.writeMessages, d.writePos)
	} else {
		_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
			d.depth,
			d.readFileNum, d.readPos,
			d.writeFileNum, d.writePos)
	}
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		d.depth = 0
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

func (d *diskQueue) moveToNextReadFile() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {

		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		oldFileInfo, _ := os.Stat(fn)

		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		} else {
			d.logf(INFO, "DISKQUEUE(%s) removed(%s) of size(%d bytes)", d.name, fn, oldFileInfo.Size())
		}

		if d.enableDiskLimitation {
			d.readMessages = 0
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to update write bytes - %s", d.name, err)
			}
		}
	}
}

func (d *diskQueue) moveForward() {
	d.depth -= 1

	if d.enableDiskLimitation {
		d.readMessages += 1
	}

	d.moveToNextReadFile()

	d.checkTailCorruption(d.depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0

		if d.enableDiskLimitation {
			d.writeMessages = 0
		}
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0
	if d.enableDiskLimitation {
		d.readMessages = 0
	}

	// significant state change, schedule a sync on the next iteration
	d.needSync = true

	d.checkTailCorruption(d.depth)
}

// This is only used for testing.
func (d *diskQueue) setRetentionTime(time time.Duration) {
	d.retentionTime = time
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		if count == d.syncEvery {
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			count++
			// moveForward sets needSync flag if a file is removed
			d.moveForward()
		case d.depthChan <- d.depth:
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
