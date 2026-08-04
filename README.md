# go-diskqueue with disk space limit

This is a fork of https://github.com/nsqio/go-diskqueue with the additional support of total disk space limit.

[![Build Status](https://travis-ci.com/kev1n80/go-diskqueue.svg?branch=master)](https://travis-ci.com/github/kev1n80/go-diskqueue) [![Go Reference](https://pkg.go.dev/badge/github.com/kev1n80/go-diskqueue.svg)](https://pkg.go.dev/github.com/kev1n80/go-diskqueue) [![GitHub release](https://img.shields.io/github/release/kev1n80/go-diskqueue.svg)](https://github.com/kev1n80/go-diskqueue/releases/latest)

A Go package providing a filesystem-backed FIFO queue

Pulled out of https://github.com/nsqio/nsq

# Description
Diskqueue is a synchronized "filesystem-backed FIFO queue” meaning it will store data you pass in by writing them to file.

Diskqueue writes messages and their message length to files in the order: message length in binary and then message. This allows Diskqueue to know how much of the file to read in order to get the next message. Once Diskqueue reads a file completely (when the number of bytes read surpasses the size of the file), it deletes the file. 

In terms of threads, creating a Diskqueue object starts a “worker thread” by calling the private function ioLoop, which is a continuous loop that accepts requests to read, write, empty, get depth, or exit. This worker thread DOES NOT create other worker threads to handle tasks asynchronously. It is important to note that Diskqueue will sync if needed (i.e. set by sync flag after user retrieves read data) before handling a new request. Using a public function can be seen as creating a request to the Diskqueue object’s “worker thread” which is implemented by using Channels. 

# Disk Space limit Feature
The original DiskQueue package did not contain a disk size limit feature; however, this forked repo does! By using a separate constructor `NewWithDiskSpace`, the user can use this disk space limit feature which will delete the oldest files in order to create space for new, incoming data.

In order to accurately adjust `depth` when a file is deleted, DiskQueue will store the number of messages in a file by writing this number to the end of the file. That way we can access this number and decrement `depth` accordingly.

Note: The disk size limit must be greater than 56 bytes which is reserved for the meta data file.

# Public Functions

## Put([]byte) error
Add data to the queue, and if a failure occurs none of the data will be written.

## ReadChan() <-chan []byte
This is expected to be an *unbuffered* channel that will not close until `Close()` or `Delete()` is called.

## Close() error
Cleans up the queue and persists the current state to metadata. 

## Delete() error
Cleans up the queue, but does not save the current state to metadata.

## Depth() int64
Returns the number of data in the queue; however, this number can become inaccurate if a file becomes corrupted or unaccessible.
Although there are times when this number can be inaccurate, this number will always be 0 when there is nothing in the queue due to the `checkTailCorruption(depth int64)` private function.

## Empty() error
Empties out the queue by deleting all of the files containing data.

## TotalBytesFolderSize() int64
Returns the total number of bytes the content in the targeted folder take up.
