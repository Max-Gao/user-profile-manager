package com.atguigu.userprofile.service;

import com.atguigu.userprofile.bean.TaskProcess;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 */
public interface TaskProcessService extends IService<TaskProcess> {


    public void updateStatus(Long taskProcessId,String status);

    public void updateStatus(Long taskProcessId,String yarnAppId,String status);

    public void genTaskProcess(String taskDate);

    public List<TaskProcess> getTodoTaskProcessList(  String taskTime);
}
