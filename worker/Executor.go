package worker

import (
	"github.com/light/crontab/common"
	"os/exec"
	"time"
)

type Executor struct {

}

var (
	G_executor Executor
)

func (executor *Executor)ExecuteJob(info common.JobExecuteInfo)  {
	go func() {
		var (
			cmd *exec.Cmd
			err error
			output []byte
			result common.JobExecuteResult
			jobLock JobLock
		)

		result = common.JobExecuteResult{
			ExecuteInfo:info,
		}

		//初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		//任务开始时间
		result.StartTime = time.Now()

		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err!= nil {
			result.Err = err
			result.EndTime = time.Now()
		}else {
			//上锁成功，重置任务开始时间
			result.StartTime = time.Now()
			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx,"/bin/bash","-c",info.Job.Command)

			//执行并捕获输出
			output,err = cmd.CombinedOutput()

			result.EndTime = time.Now()
			result.Output = output
			result.Err = err

		}
		G_scheduler.PushJobResult(result)


	}()
}

func InitExecutor() error {
	G_executor = Executor{}
	return nil
}