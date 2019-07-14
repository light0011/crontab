package worker

import (
	"context"
	"github.com/light/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)
// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr JobMgr
)

func (jobManager *JobMgr)watchKiller()  {
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
		job *common.Job
	)

	go func() {
		//监听/cron/killer目录的变化
		watchChan = jobManager.watcher.Watch(context.TODO(),common.JOB_KILLER_DIR,clientv3.WithPrefix())
		for watchResp = range watchChan{
			for _,watchEvent = range watchResp.Events{
				switch watchEvent.Type {
				case mvccpb.PUT:
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{Name:jobName}
					*jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL,*job)
					G_scheduler.PushJobEvent(*jobEvent)
				}
			}
		}
	}()

}

func (jobManager *JobMgr) watchJobs() error {
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue

		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent common.JobEvent
		err error
	)

	//获取 /cron/jobs下所有对的任务,并且获知当前集群的revision
	if getResp,err = jobManager.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix()) ;err!=nil{
		return err
	}

	//查看当前任务
	for _,kvpair = range getResp.Kvs{
		//反序列化
		if job,err = common.UnpackJob(kvpair.Value);err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,*job)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	//从该版本开始监听变化事件
	go func() {
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = jobManager.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())

		for watchResp = range watchChan{
			for _,watchEvent = range watchResp.Events{
				switch watchEvent.Type {
				case mvccpb.PUT:
					if job,err = common.UnpackJob(watchEvent.Kv.Value);err != nil {
						continue
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,*job)
				case mvccpb.DELETE:
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))

					job = &common.Job{
						Name:jobName,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE,*job)
				}
				G_scheduler.PushJobEvent(jobEvent)

			}
		}





	}()

	return nil

}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}

	G_jobMgr.watchJobs()





	return
}

// 创建任务执行锁
func (jobMgr JobMgr) CreateJobLock(jobName string) JobLock{
	return InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
}