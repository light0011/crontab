package master

import (
	"encoding/json"
	"fmt"
	"github.com/light/crontab/common"
	"net/http"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var G_apiServer *ApiServer

func handleJobSave(w http.ResponseWriter,r *http.Request)  {
	var (
		job common.Job
		oldJob common.Job
		err error
		postJob string

	)
	if err = r.ParseForm();err != nil{
		goto ERR
		return
	}
	postJob = r.PostForm.Get("job")

	if err = json.Unmarshal([]byte(postJob),&job); err!=nil{
		goto ERR
		return
	}

	//保存到etcd
	if oldJob,err =  G_jobManager.SaveJob(job);err!= nil{
		fmt.Println(err)
		goto ERR
		return
	}

	w.Write(common.BuildResponse(1,"成功",oldJob))
	return
ERR:
	w.Write(common.BuildResponse(-1,err.Error(),nil))


}

// 删除任务接口
// POST /job/delete   name=job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err error	// interface{}
		name string
		oldJob common.Job
	)

	// POST:   a=1&b=2&c=3
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 删除的任务名
	name = req.PostForm.Get("name")

	// 去删除任务
	if oldJob, err = G_jobManager.DeleteJob(name); err != nil {
		goto ERR
	}

	// 正常应答
	resp.Write(common.BuildResponse(0, "success", oldJob))
	return

ERR:
	resp.Write(common.BuildResponse(-1, err.Error(), nil))
}

// 列举所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		jobList []common.Job
		err error
	)

	// 获取任务列表
	if jobList, err = G_jobManager.ListJobs(); err != nil {
		goto ERR
	}

	// 正常应答
	resp.Write(common.BuildResponse(0, "success", jobList))
	return

ERR:
	resp.Write(common.BuildResponse(-1, err.Error(), nil))

}

// 强制杀死某个任务
// POST /job/kill  name=job1
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
	)

	// 解析POST表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 要杀死的任务名
	name = req.PostForm.Get("name")

	// 杀死任务
	if err = G_jobManager.KillJob(name); err != nil {
		goto ERR
	}

	// 正常应答
	resp.Write(common.BuildResponse(0, "success", nil))

	return

ERR:
	resp.Write(common.BuildResponse(-1, err.Error(), nil))
}



// 获取健康worker节点列表
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err error
	)

	if workerArr, err = G_workerManager.ListWorkers(); err != nil {
		goto ERR
	}

	// 正常应答
	resp.Write(common.BuildResponse(0, "success", workerArr))

	return

ERR:
	resp.Write(common.BuildResponse(-1, err.Error(), nil))
}

func InitApiServer() error {
	mux := http.NewServeMux()

	//静态资源路径
	staticDir := http.Dir(G_config.WebRoot)
	staticHandler := http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",staticHandler))
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/worker/list", handleWorkerList)

	//启动监听
	httpServer := http.Server{
		ReadTimeout:time.Duration(G_config.ApiReadTimeout) * time.Microsecond,
		WriteTimeout:time.Duration(G_config.ApiWriteTimeout) * time.Microsecond,
		Handler:mux,
		Addr:G_config.ApiPort,
	}

	err := httpServer.ListenAndServe()
	return err

}