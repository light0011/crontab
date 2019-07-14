package common

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 定时任务
type Job struct {
	Name string `json:"name"`	//  任务名
	Command string	`json:"command"` // shell命令
	CronExpr string	`json:"cronExpr"`	// cron表达式
}

type JobEvent struct {
	EventType int
	Job Job
}

type JobSchedulePlan struct {
	Job Job
	Expr cronexpr.Expression
	NextTime time.Time
}

type JobExecuteInfo struct {
	Job Job
	PlanTime time.Time
	RealTime time.Time
	CancelCtx context.Context
	CancelFunc context.CancelFunc
}

type JobExecuteResult struct {
	ExecuteInfo JobExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

func UnpackJob(value []byte)(*Job,error)  {
	var (
		job Job
		err error
	)
	if err = json.Unmarshal(value,&job) ;err!=nil{
		return nil,err
	}
	return &job,nil

}

func ExtractJobName(jobKey string) (string) {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 提取worker的IP
func ExtractWorkerIP(regKey string) (string) {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}

func BuildJobSchedulePlan(job *Job)(*JobSchedulePlan,error)  {
	var (
		expr *cronexpr.Expression
		err error
		jobSchedulePlan *JobSchedulePlan
	)

	// 解析JOB的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return nil,err
	}

	// 生成任务调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job: *job,
		Expr: *expr,
		NextTime: expr.Next(time.Now()),
	}
	return jobSchedulePlan,nil
}

func BuildResponse(errno int,msg string,data interface{}) []byte {
	response := Response{
		Errno:errno,
		Msg:msg,
		Data:data,
	}
	fmt.Println(response)
	resp,_ := json.Marshal(response)
	return resp
}

func BuildJobExecuteInfo(jobSchedulePlan JobSchedulePlan) JobExecuteInfo  {
	jobExecuteInfo := JobExecuteInfo{
		Job:jobSchedulePlan.Job,
		PlanTime:jobSchedulePlan.NextTime,
		RealTime:time.Now(),
	}
	jobExecuteInfo.CancelCtx,jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return jobExecuteInfo
}

func BuildJobEvent(eventType int,job Job)JobEvent  {
	return JobEvent{
		EventType:eventType,
		Job:job,
	}
}

