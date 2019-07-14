package worker

import (
	"fmt"
	"github.com/light/crontab/common"
	"time"
)

type Scheduler struct {
	jobEventChan chan common.JobEvent
	jobPlanTable map[string]*common.JobSchedulePlan
	jobExecutingTable map[string]common.JobExecuteInfo
	jobResultChan chan common.JobExecuteResult

}

func (scheduler *Scheduler)handleJobResult(result common.JobExecuteResult) {
	delete(scheduler.jobExecutingTable,result.ExecuteInfo.Job.Name)
	//TODO 记录执行日志
	fmt.Printf("执行结果 %+v \n",result)
	fmt.Println("执行结果输出 ",string(result.Output))
}

func (scheduler *Scheduler) tryStartJob(jobPlan common.JobSchedulePlan)  {
	//如果正在执行 则跳过
	var (
		jobExecuteInfo common.JobExecuteInfo
		jobExecuting bool
	)
	if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting{
		return
	}

	//构建执行状态
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//开始执行任务
	fmt.Println("执行任务:",jobExecuteInfo.Job.Name,jobExecuteInfo.PlanTime)

	G_executor.ExecuteJob(jobExecuteInfo)
}

func (scheduler *Scheduler) TrySchedule() time.Duration {
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	if len(scheduler.jobPlanTable) == 0 {
		return time.Second
	}

	now = time.Now()
	for _,jobPlan = range scheduler.jobPlanTable{
		fmt.Println(jobPlan.NextTime.Format("2006-01-02 15:04:05"))
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			scheduler.tryStartJob(*jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}
		//计算最近一个要过期的时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	//下次调度时间
	return (*nearTime).Sub(now)

}

func (scheduler *Scheduler)handleJobEvent(jobEvent common.JobEvent)  {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
		jobExisted bool
		err error
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan,err = common.BuildJobSchedulePlan(&jobEvent.Job);err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
		if jobSchedulePlan,jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted {
			delete(scheduler.jobPlanTable,jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		if *jobExecuteInfo ,jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name];jobExecuting{
			jobExecuteInfo.CancelFunc()
		}
		
	}
}

func (scheduler *Scheduler) scheduleLoop()  {

	var (
		jobEvent common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult common.JobExecuteResult
	)
	//初始化
	scheduleAfter = scheduler.TrySchedule()

	//调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	for {
		select {
		case <-scheduleTimer.C: //最近的任务到期了
			scheduleAfter = scheduler.TrySchedule()
			scheduleTimer.Reset(scheduleAfter)
		case jobResult = <- scheduler.jobResultChan: //记录脚本执行结果
			scheduler.handleJobResult(jobResult)
		case jobEvent = <- scheduler.jobEventChan: //监听任务执行结果
			scheduler.handleJobEvent(jobEvent)


		}
	}


}


var G_scheduler Scheduler

func InitScheduler() error {
	G_scheduler = Scheduler{
		jobEventChan:make(chan common.JobEvent,1000),
		jobPlanTable:make(map[string]*common.JobSchedulePlan),
		jobExecutingTable:make(map[string]common.JobExecuteInfo),
		jobResultChan:make(chan common.JobExecuteResult,1000),
	}
	go G_scheduler.scheduleLoop()

	return nil
}

func (scheduler *Scheduler) PushJobResult(jobResult common.JobExecuteResult)  {
	scheduler.jobResultChan <- jobResult
}


func (scheduler *Scheduler) PushJobEvent(jobEvent common.JobEvent)  {
	scheduler.jobEventChan <- jobEvent
}

