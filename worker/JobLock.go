package worker

import (
	"context"
	"github.com/light/crontab/common"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
)

type JobLock struct {
	//etcd客户端
	kv clientv3.KV
	lease clientv3.Lease

	jobName string
	cancelFunc context.CancelFunc
	leaseId clientv3.LeaseID
	isLocked bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) JobLock {
	return JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
}

func (jobLock *JobLock)TryLock() error {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <- chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
		err error
	)

	//创建租约
	if leaseGrantResp,err =  jobLock.lease.Grant(context.TODO(),5);err != nil{
		return err
	}

	//取消自动续租
	cancelCtx,cancelFunc = context.WithCancel(context.TODO())

	//租约id
	leaseId = leaseGrantResp.ID

	//自动续租
	if keepRespChan,err = jobLock.lease.KeepAlive(cancelCtx,leaseId);err!= nil {
		goto FAIL
	}

	//处理续租应答的协程
	go func() {
		var keepResp *clientv3.LeaseKeepAliveResponse
		for  {
			select {
			case keepResp = <- keepRespChan:
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	//创建事务
	txn = jobLock.kv.Txn(context.TODO())

	//锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	//事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey),"=",0)).
		Then(clientv3.OpPut(lockKey,"",clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	//提交事务
	if txnResp,err = txn.Commit();err != nil {
		goto FAIL
	}

	//返回失败,失败是否租约
	if !txnResp.Succeeded {
		return errors.New("锁已被占用")
		goto FAIL
	}

	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true


	return err

FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(),leaseId)
	return err
}

func (jobLock *JobLock)Unlock()  {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(),jobLock.leaseId)
	}
}
