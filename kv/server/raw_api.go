package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	res := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: val == nil,
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify := storage.Modify{
		Data: put,
	}
	res := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{
		Data: delete,
	}
	res := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	res := &kvrpcpb.RawScanResponse{}
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	iter := reader.IterCF(req.Cf)
	defer reader.Close()
	defer iter.Close()

	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item.ValueSize() == 0 {
			continue
		}
		val, err := item.Value()
		if err != nil {
			res.Error = err.Error()
			return res, err
		}
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		if len(res.Kvs) >= int(req.Limit) {
			break
		}
	}
	return res, nil
}
