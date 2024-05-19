package com.atguigu.userprofile.service;

import com.atguigu.userprofile.bean.TagCommonTask;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 */
public interface TagCommonTaskService extends IService<TagCommonTask> {


    public  TagCommonTask getTagCommonTaskWithJarFile(Long id);
}
