package com.atguigu.userprofile.mapper;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Service;

import javax.ws.rs.DELETE;
import java.util.List;


@Mapper
@DS("mysql")
public interface UserGroupMapper extends BaseMapper<UserGroup> {



    @Insert("${SQL}")
    @DS("clickhouse")
    public  void  insertUserGroupBitmap(@Param("SQL") String  insertSQL );

    @Select("select bitmapCardinality( us)  from user_group where user_group_id=#{id} ")
    @DS("clickhouse")
    public Long getUserGroupCount(@Param("id") String userGroupId);

    @Select(" select  arrayJoin( bitmapToArray(us) )  uids from user_group where user_group_id=#{id} ")
    @DS("clickhouse")
    public  List<String>   getUserGroupUids(@Param("id") String userGroupId);
}
