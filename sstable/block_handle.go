package sstable

type BlockHandle struct {
	Offset int
	Size   int
}

type IndexBlockHandle struct {
	LastKey []byte
	BlockHandle
}

type Footer struct {
	metaIndexHandle BlockHandle
	indexHandle     BlockHandle
}
