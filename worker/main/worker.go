package main

import (
	"flag"
	"fmt"
	"github.com/light/crontab/worker"
	"runtime"
	"time"
)

var (
	configFile string
)

//初始化命令行
func initArgs()  {
	flag.StringVar(&configFile,"config","./worker/main/worker.json","输入worker.json的路径")
	flag.Parse()
}

//初始化线程数量
func initEnv()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main()  {

	var err error
	initArgs()
	initEnv()

	//初始化配置
	if err := worker.InitConfig(configFile);err !=nil{
		fmt.Println(err)
		return
	}

	//服务注册
	if  err =  worker.InitRegister();err != nil{
		return
	}

	//启动执行器
	if err = worker.InitExecutor();err != nil {
		goto ERR
	}

	//初始化调度
	if err = worker.InitScheduler();err != nil {
		goto ERR
	}

	//初始化任务管理器
	if err = worker.InitJobMgr();err != nil {
		goto ERR
	}


	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}


ERR:
	fmt.Println(err)



}


