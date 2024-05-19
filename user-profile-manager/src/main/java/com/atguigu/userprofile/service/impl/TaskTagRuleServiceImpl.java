package com.atguigu.userprofile.service.impl;

import com.atguigu.userprofile.bean.TaskTagRule;
import com.atguigu.userprofile.mapper.TaskTagRuleMapper;
import com.atguigu.userprofile.service.TaskTagRuleService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>

 */
@Service
@DS("mysql")
public class TaskTagRuleServiceImpl extends ServiceImpl<TaskTagRuleMapper, TaskTagRule> implements TaskTagRuleService {

}
