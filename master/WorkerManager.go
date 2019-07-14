package master

import (
	"context"
	"github.com/light/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"time"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type WorkerManager struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var G_workerManager WorkerManager

func InitWorkerManager() error {


	config := clientv3.Config{
		Endpoints:G_config.EtcdEndpoints,
		DialTimeout:time.Duration(G_config.EtcdDialTimeout)*time.Microsecond,
	}
	client,err :=  clientv3.New(config)

	if  err != nil{
		return err
	}
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)


	G_workerManager = WorkerManager{
		client:client,
		kv:kv,
		lease:lease,
	}


	return nil
}


// 获取在线worker列表
func (workerMgr *WorkerManager) ListWorkers() (workerArr []string, err error) {
	var (
		getResp *clientv3.GetResponse
		kv *mvccpb.KeyValue
		workerIP string
	)

	// 初始化数组
	workerArr = make([]string, 0)

	// 获取目录下所有Kv
	if getResp, err = workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}


	// 解析每个节点的IP
	for _, kv = range getResp.Kvs {
		// kv.Key : /cron/workers/192.168.2.1
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}
