package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	opts badger.Options
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	standAloneStorage := &StandAloneStorage{
		opts: opts,
		db:   nil,
	}
	return standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db, err := badger.Open(s.opts)
	if err != nil {
		log.Error(err)
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if nil != s.db {
		s.db.Close()
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			key := engine_util.KeyWithCF(modify.Cf(), modify.Key())
			var err error
			switch modify.Data.(type) {
			case storage.Put:
				err = txn.Set(key, modify.Value())
			case storage.Delete:
				err = txn.Delete(key)
			}
			if err != nil {
				log.Error(err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil // key 不存在返回 nil，不是错误
		}
		log.Error(err)
		return nil, err
	}
	if item.IsDeleted() || item.ValueSize() == 0 {
		return nil, nil
	}
	value, err := item.Value()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return value, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	// Discard txn.
	r.txn.Discard()
}
