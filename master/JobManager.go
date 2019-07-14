package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/light/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobManager struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var G_jobManager JobManager

func InitJobManager() error {


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


	G_jobManager = JobManager{
		client:client,
		kv:kv,
		lease:lease,
	}


	return nil
}

func (jobManager *JobManager)SaveJob(job common.Job) (common.Job,error) {

	var (
		oldJob common.Job
		jobValue []byte
		err error
		putResp *clientv3.PutResponse
	)

	jobKey := common.JOB_SAVE_DIR+job.Name
	if jobValue,err = json.Marshal(job);err != nil {
		return oldJob,err
	}

	if putResp,err = jobManager.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err !=nil{
		fmt.Println(err)
		return oldJob,err
	}

	fmt.Println(putResp)
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value,&oldJob);err !=nil {
			return oldJob,err
		}
	}
	return oldJob,nil

	


}




// 删除任务
func (jobManager *JobManager) DeleteJob(name string) (oldJob common.Job, err error) {
	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	// etcd中保存任务的key
	jobKey = common.JOB_SAVE_DIR + name

	// 从etcd中删除它
	if delResp, err = jobManager.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		// 解析一下旧值, 返回它
		if err =json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = oldJobObj
	}
	return
}

// 列举任务
func (jobManager *JobManager) ListJobs() (jobList []common.Job, err error) {
	var (
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job common.Job
	)

	// 任务保存的目录
	dirKey = common.JOB_SAVE_DIR

	// 获取目录下所有任务信息
	if getResp, err = jobManager.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化数组空间
	jobList = make([]common.Job, 0)
	// len(jobList) == 0

	// 遍历所有任务, 进行反序列化
	for _, kvPair = range getResp.Kvs {
		job = common.Job{}
		if err =json.Unmarshal(kvPair.Value, &job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobManager *JobManager) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	// 通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	if leaseGrantResp, err = jobManager.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobManager.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}





