package com.atguigu.userprofile.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.constants.ConstCodes;
import com.atguigu.userprofile.mapper.UserGroupMapper;
import com.atguigu.userprofile.service.TagInfoService;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.utils.RedisUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;

import org.apache.catalina.User;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *

 */
@Service
@Slf4j
@DS("mysql")
public class UserGroupServiceImpl extends ServiceImpl<UserGroupMapper, UserGroup> implements UserGroupService {



    @Autowired
    TagInfoService tagInfoService;

    @Override
    public void saveUserGroupInfo(UserGroup userGroup) {
        // 1 把标签条件提取json
        List<TagCondition> tagConditions = userGroup.getTagConditions();
        String conditionJson = JSON.toJSONString(tagConditions);
        // UserGroup userGroup1 = JSON.parseObject(userGroupJson, UserGroup.class);//把json转为一个对象
       // List<TagCondition> userGroupList = JSON.parseArray(conditionJson, TagCondition.class);//把json转为一个集合

        userGroup.setConditionJsonStr(conditionJson);
        //  2  把标签条件提取为中文说明
        String conditionComment = userGroup.conditionJsonToComment();
        userGroup.setConditionComment(conditionComment);
        // 3 添加创建时间
        userGroup.setCreateTime(new Date());

        //4 保存 到mysql
        saveOrUpdate(userGroup);


    }



    // 通过查询元数据获得 人群包信息 并且写入到clickhouse中

    /**
     *
     *insert into xxx
     *   select bitmapAnd(
     *     (select groupBitmapMergeState(us) from user_tag_value_string where tag_code = 'tg_person_base_gender' and tag_value = '男')
     *     ,
     *     (select groupBitmapMergeState(us) from user_tag_value_string where tag_code = 'tg_person_base_agegroup' and tag_value in ('90后'))
     *   )
     */

    //单条子查询  =》 bitmapAnd 组合多条子查询  =》select =》 insert
    public  void genUserGroupUids( UserGroup userGroup){
        Map<String, TagInfo> tagInfoMapWithCode = tagInfoService.getTagInfoMapWithCode();
        String bitmapSQL = getBitmapSQL(userGroup.getTagConditions(), tagInfoMapWithCode, userGroup.getBusiDate());
        insertBitmap(bitmapSQL,userGroup.getId());

        //更新人数
        Long userGroupCount = baseMapper.getUserGroupCount(userGroup.getId().toString());
        userGroup.setUserGroupNum(userGroupCount);
        saveOrUpdate(userGroup);

    }

    public void genUserGroupRedis(UserGroup userGroup){
        //从clickhouse中把用户集合读取出来
        List<String> userGroupUids = baseMapper.getUserGroupUids(userGroup.getId().toString());

        // 写入user_group集合
        Jedis jedis = RedisUtil.getJedis();
        // type?    set       key?   user_group:[user_group_id]   score/field? 无    value?   uids
        //   写api?  sadd    读api? sismember 或 smembers       expire?  不是缓存  无
        String userGroupKey="user_group:"+userGroup.getId().toString();
        String[] uids = userGroupUids.toArray(new String[]{});
        jedis.sadd(userGroupKey, uids);

        jedis.close();


    }



    public void insertBitmap(String  bitmapSQL , Long userGroupId ){
         String  insertSQL=" insert into  user_group  " +
                 " select   '"+userGroupId+"' as user_group_id ," +bitmapSQL +" as us "   ;
        System.out.println(insertSQL);
        baseMapper.insertUserGroupBitmap(insertSQL);

    }




    //通过bitmapAnd函数拼接多条子查询
    //      bitmapAnd(
    //          (bitmapAnd(
    //                      (subquery1) ,
    //                      (subquery2)
    //                     )
    //          )
    //          ,
    //          (sub3 )
    //      )
    private   String  getBitmapSQL( List<TagCondition> tagConditionList, Map<String ,TagInfo> tagInfoMap, String busiDate ){
        StringBuilder sqlStringBuilder=new StringBuilder();
        for (TagCondition tagCondition : tagConditionList) {
            String subQuerySQL=getSubQueryBitmapSQL(tagCondition,tagInfoMap,busiDate);
            if(sqlStringBuilder.length()==0){
                sqlStringBuilder.append(subQuerySQL);
            }else{
                sqlStringBuilder.insert(0," bitmapAnd( (" ).append("),(").append(subQuerySQL).append("))");
            }

        }
        return sqlStringBuilder.toString();

    }



    //查询单条子sql
   //select  groupBitmapMergeState( us ) from user_tag_value_string
    //  where  tag_code='TG_PERSON_BASE_GENDER'  and tag_value='男'   and dt='2021-05-16'
    //
    // tableName ==>    tagCode      == 查询mysql  ==>tagInfo ==>tagValueType
    // tagcode ==> 现成
    // tagvalue ==>      根据tagValueType  来判断是否加单引  如果是in 或者 notin加括号 如果数字不加单引 字符串加单引
                         // 如果是多个  ('','')      tagValues.size>0
     // operator ==>   把operator 做一下转义
    //    dt==>busidate
    private  String  getSubQueryBitmapSQL(TagCondition tagCondition,Map<String ,TagInfo> tagInfoMap,String busiDate){
        // tableName ==>    tagCode      == 查询mysql  ==>tagInfo ==>tagValueType
            String tagCode=tagCondition.getTagCode();
           TagInfo tagInfo = tagInfoMap.get(tagCode);
          String tagValueType = tagInfo.getTagValueType();
          String tableName=null;
          if(tagValueType.equals( ConstCodes.TAG_VALUE_TYPE_STRING)){
              tableName="user_tag_value_string";
          }else if(tagValueType.equals( ConstCodes.TAG_VALUE_TYPE_LONG)){
            tableName="user_tag_value_long";
          }else if(tagValueType.equals( ConstCodes.TAG_VALUE_TYPE_DECIMAL)){
              tableName="user_tag_value_decimal";
          }else if(tagValueType.equals( ConstCodes.TAG_VALUE_TYPE_DATE)){
              tableName="user_tag_value_date";
          }
        // tagvalue ==>      根据tagValueType  来判断是否加单引  如果是in 或者 notin加括号 如果数字不加单引 字符串加单引
        // 如果是多个  ('','')      tagValues.size>0

        List<String> tagValues = tagCondition.getTagValues();
        String tagValuesStr=null;
        if(ConstCodes.TAG_VALUE_TYPE_STRING.equals(tagValueType) || ConstCodes.TAG_VALUE_TYPE_DATE.equals(tagValueType) ){
              tagValuesStr =    "'"+StringUtils.join(tagValues, "','")+"'";
        }else{
              tagValuesStr =   StringUtils.join(tagValues, ",");
        }
        if(tagValues.size()>1){
            tagValuesStr="("+tagValuesStr+")";
        }

        String conditionOperator = getConditionOperator(tagCondition.getOperator());

        String sql = "select  groupBitmapMergeState( us ) from " + tableName +
                "  where  tag_code='" + tagCode.toLowerCase() + "'  and tag_value" + conditionOperator + " " + tagValuesStr + "  and dt='"+busiDate+"'";



       return sql;

    }


    // 把中文的操作代码转换为 判断符号
    private  String getConditionOperator(String operator){
        switch (operator){
            case "eq":
                return "=";
            case "lte":
                return "<=";
            case "gte":
                return ">=";
            case "lt":
                return "<";
            case "gt":
                return ">";
            case "neq":
                return "<>";
            case "in":
                return "in";
            case "nin":
                return "not in";
        }
        throw  new RuntimeException("操作符不正确");
    }

}
