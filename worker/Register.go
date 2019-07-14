package worker

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"

	"github.com/light/crontab/common"
	"net"
	"time"
)

// 注册节点到etcd： /cron/workers/IP地址
type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string // 本机IP
}

var (
	G_register *Register
)

func (register *Register)keepOnline()  {

	var (

		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <- chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc

	)


	for  {
		regKey = common.JOB_WORKER_DIR+register.localIP
		
		cancelFunc = nil
		
		//创建租约
		if leaseGrantResp,err = register.lease.Grant(context.TODO(),10);err != nil{
			goto RETRY
		}
		
		//自动续租
		if keepAliveChan,err = register.lease.KeepAlive(context.TODO(),leaseGrantResp.ID);err != nil {
			goto RETRY
		}

		cancelCtx,cancelFunc = context.WithCancel(context.TODO())

		//注册到etcd
		if _,err = register.kv.Put(cancelCtx,regKey,"",clientv3.WithLease(leaseGrantResp.ID)) ;err != nil{
			goto RETRY
		}

		//处理续租应答
		for  {
			select {
			case keepAliveResp = <- keepAliveChan:
				if keepAliveResp == nil{ //续租失败
					goto RETRY
				}
			}
		}
		
		RETRY:
		//相当于重新开始
		time.Sleep(time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
		fmt.Println(regKey)
	}

}

func getLocalIP()(string,error)  {
	adds,err := net.InterfaceAddrs()
	if err != nil {
		return "",err
	}
	for _,addr := range adds{
		 ipNet,isIpNet :=  addr.(*net.IPNet)
		if isIpNet && !ipNet.IP.IsLoopback(){
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(),nil
			}
		}

	}
	return "",nil
}

func InitRegister() error {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIp string
		err error
	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return err
	}

	// 本机IP
	if localIp, err = getLocalIP(); err != nil {
		return err
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		client: client,
		kv: kv,
		lease: lease,
		localIP: localIp,
	}
	go G_register.keepOnline()

	return nil
}

