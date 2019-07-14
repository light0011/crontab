package main

import (
	"flag"
	"fmt"
	"github.com/light/crontab/master"
	"runtime"
)

var (
	configFile string
)

//初始化命令行
func initArgs()  {
	flag.StringVar(&configFile,"config","./master/main/master.json","输入master.json的路径")
	flag.Parse()
}

//初始化线程数量
func initEnv()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main()  {
	initArgs()
	initEnv()

	//初始化配置
	if err := master.InitConfig(configFile);err !=nil{
		fmt.Println(err)
		return
	}

	//初始化
	if err := master.InitWorkerManager();err !=nil{
		fmt.Println(err)
		return
	}

	//初始化任务管理器
	if err := master.InitJobManager();err !=nil{
		fmt.Println(err)
		return
	}

	//启动端口
	master.InitApiServer()




}


