#!/usr/bin/python
# -*-coding:utf-8 -*-
# date: 2021-11-19
# author: zzs

import datetime
import json
import re
import time
import traceback

from isopGxdx.utils.common import getLogger
from isopGxdx.utils.log_dict import protocol_map, app_proto_map
from isopGxdx.utils import log_dict
from dataInterface.functions import CFunction
from dataInterface.db.params import CPgSqlParam
from isopGxdx.validation import validate_ipv4, validate_ipv6, validate_port, is_mask, validateIpFieldAndformCondition

logger = getLogger('illegal_event_rule.log')


class IllegalEventRule(object):
    """

    支持选定特定的自定义规则，生成对应的事件展示页签，如自定义规则1，对应事件页签1，
    自定义规则2，对应事件页签2等
    """

    def validate_conditions(self, params_key, params_value):
        store_map_value = []
        for key, value in protocol_map.items():
            store_map_value.append(value)
        flag = True
        msg = ""
        if params_key == "sip" or params_key == "dip":
            if "-" not in params_value:  # 表示单一的ip
                conditions, errFormat = validateIpFieldAndformCondition(params_value)
                if not conditions and errFormat:
                    flag = False
                    msg = "ip有误"
                    return flag, msg
                if len(params_value.split(".")) != 4:
                    flag = False
                    msg = "ip有误"
                    return flag, msg
            elif "/" in params_value:
                flag = False
                msg = "请输入合法ip,如：10.67.1.1-10.67.2.2"
                return flag, msg
            else:
                detail_value_list = params_value.split("-")
                conditions1, errFormat1 = validateIpFieldAndformCondition(detail_value_list[0])
                conditions2, errFormat2 = validateIpFieldAndformCondition(detail_value_list[1])
                if not conditions1 and errFormat1 and not conditions2 and errFormat2:
                    flag = False
                    msg = "ip有误"
                    return flag, msg
                if len(detail_value_list[0].split(".")) != 4 or len(detail_value_list[1].split(".")) != 4:
                    flag = False
                    msg = "ip有误"
                    return flag, msg
        elif params_key == "sport" or params_key == "dport":
            if "," not in params_value:  # 表示单一的port
                if validate_port(params_value):
                    flag = False
                    msg = "端口有误"
                    return flag, msg
            else:
                detail_value_list = params_value.split(",")
                for detail_value in detail_value_list:
                    if validate_port(detail_value):
                        flag = False
                        msg = "端口有误"
                        return flag, msg
        elif params_key == "protocol":  # 校验协议
            if "," in params_value:
                detail_value_list = params_value.split(",")
                for detail_value in detail_value_list:
                    if detail_value not in store_map_value:
                        flag = False
                        msg = "协议有误"
                        return flag, msg
            else:
                if params_value not in store_map_value:
                    flag = False
                    msg = "协议有误"
                    return flag, msg
        return flag, msg

    def add_custome_rule(self, custom_rule):
        """
        支持选定特定的自定义规则
        生成自定义规则后保存在pg,
        """

        try:
            re_string = "~!@#$%^&*()+*/<>,[]\/ 。，;；？?{}【】"
            logger.info("custom_rule------------------：", custom_rule)
            print "custom_rule0--------------", custom_rule
            for k, v in custom_rule.items():
                print k

                if k in ["sip", "dip"]:
                    print 234234, k, v
                    param_value = v['value']
                    if param_value:
                        flag, msg = self.validate_conditions(k, param_value)
                        if not flag:
                            print 123123
                            return {"ret": flag, 'msg': msg}
                elif k in ['sport', 'dport'] and v != '':
                    print "type---", v
                    print "type---", type(v)
                    flag, msg = self.validate_conditions(k, v)
                    if not flag:
                        return {"ret": flag, 'msg': msg}
                elif k in ['protocol'] and v != '':
                    flag, msg = self.validate_conditions(k, v)
                    if not flag:
                        return {"ret": flag, 'msg': msg}

            rule_name = custom_rule.get('rule_name', '')
            start_time = custom_rule.get('start_time', '')
            end_time = custom_rule.get('end_time', '')
            sip = custom_rule.get('sip', '')
            dip = custom_rule.get('dip', '')
            print "dip--", dip
            sport = custom_rule.get('sport', '')
            print "sport--", sport
            dport = custom_rule.get('dport', '')
            protocol = custom_rule.get('protocol', '')
            protocol_dict = custom_rule['protocol_dict']
            if int(start_time) > int(end_time):
                return {"ret": False, 'msg': '开始时间不能大于结束时间'}
            if not rule_name:
                return {"rule_name_msg": "自定义规则名称长度不能为空"}
            # 判断rule_name是否重复
            query_name_sql = """
            select rule_name from internal_app_gxdx.custom_rule where rule_name = '%s'
            """ % rule_name
            if json.loads(CFunction.execute(CPgSqlParam(query_name_sql))):
                print rule_name, 12
                return {"rule_name_msg": "自定义规则名称不能重复"}
            if len(rule_name) > 20:
                return {"rule_name_msg": "自定义规则名称长度不能超过20"}
            rule_name_list = list(rule_name)
            re_string_list = list(re_string)
            tmp = [val for val in rule_name_list if val in re_string_list]
            print "tmp-------", tmp
            logger.info("tmp--1-{}".format(tmp))
            if tmp:
                logger.info("tmp---{}".format(tmp))
                return {"rule_name_msg": "自定义规则名称不能包含特殊字符"}

            rule_retails = {
                "sip": sip,
                "dip": dip,
                "sport": str(sport),
                "dport": str(dport),
                "protocol": protocol,
                # sip":{"key":"is", "value":"120.0.0.1"}
            }
            print "rule_retails0000000000000", rule_retails
            con = ''
            for k, v in rule_retails.items():
                print "k,k,k,-----------", k, v

                if "sip" == k and v != '':
                    if v['key'] == 'is':
                        if v['value']:
                            con += k + "=" + v['value'] + " and "
                    else:
                        if v['value']:
                            con += k + "!=" + v['value'] + " and "
                elif "dip" == k and v != '':
                    if v['key'] == 'is':
                        if v['value']:
                            con += k + "=" + v['value'] + " and "
                    else:
                        if v['value']:
                            con += k + "!=" + v['value'] + " and "

                elif "sport" == k and v != '':
                    con += k + "=" + v + " and "

                elif "dport" == k and v != '':
                    con += k + "=" + v + " and "

                elif "protocol" == k and v != '':
                    con += k + "=" + v + " and "

            print "con -----------+++++++++", con, type(con)
            print [rule_name, con, int(start_time), int(end_time)]
            custom_rule_sql = """
                           insert into internal_app_gxdx.custom_rule (rule_name, rule_details, start_time, end_time, protocol_dict) 
                           values('%s', '%s', %d, %d, '%s')
                       """ % (rule_name, con, start_time, end_time, json.dumps(protocol_dict))
            print custom_rule_sql

            CFunction.execute(CPgSqlParam(custom_rule_sql))
            print '创建自定义规则成功'
            return {"ret": True, 'msg': '创建自定义规则成功'}

        except Exception as e:
            print (traceback.format_exc())
            logger.error(e.message)
            logger.error(traceback.format_exc())

    def delete_custom_rule(self, custom_rule_name):
        """
        删除自定义标签
        """
        try:
            rule_name_list = []
            if not custom_rule_name:
                return {"msg": "规则名称不能为空"}

            rule_sql = """ select rule_name from internal_app_gxdx.illegal_rule_group 
            """
            rule_list = json.loads(CFunction.execute(CPgSqlParam(rule_sql)))
            print "rule_list----", rule_list
            logger.info("custom_rule_name---{}".format(custom_rule_name))
            if rule_list:
                for item in rule_list:
                    rule_name_list.append(item[0])
                if custom_rule_name in rule_name_list:
                    logger.info("custom_rule_name--5555-{}".format(custom_rule_name))
                    return {"status": 200, "msg": "删除失败，请先删除违规访问事件报表中对应周期任务"}

            delete_sql = """
                delete from internal_app_gxdx.custom_rule  where rule_name = '%s'
            """ % custom_rule_name
            logger.info("delete_sql---{}".format(delete_sql))
            print delete_sql
            CFunction.execute(CPgSqlParam(delete_sql))
            return {"status": 200, "msg": "删除规则成功"}
        except Exception as e:
            logger.error("delete success")
            logger.error(str(e))

    def get_events_by_rules(self, rule_name, page_size, page_index):
        """
        展示违规访问事件列表:
        1.根据自定义规则的规则名称查询规则，
        2.根据规则查询违规事件
        :param params_dict:
        :return:
        """
        result_list1 = []
        result_list = []
        result_list_dict = {}

        try:
            if not rule_name:
                result_list_dict = {'msg': "自定义标签名不能为空"}
                return result_list_dict
            print "self.name:  ---", rule_name
            from_index = (page_index - 1) * page_size
            get_rule_sql = """
                select rule_details, start_time, end_time from internal_app_gxdx.custom_rule   where rule_name = '%s'  
            """ % rule_name
            get_rule_datas = json.loads(CFunction.execute(CPgSqlParam(get_rule_sql)))
            print "get_rule_datas------------", get_rule_datas
            if get_rule_datas:
                for item in get_rule_datas:
                    print "item ==============", item
                    rule_details = item[0]
                    start_ = time.localtime(int(item[1]))
                    print item[1], type(int(item[1]))

                    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", start_)
                    end_ = time.localtime(int(item[2]))
                    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", end_)
                time_condition = "start_time >='%s' and end_time<='%s'" % (start_time_str, end_time_str)
                print "time_condition-----------", time_condition
                if rule_details:
                    # 拼接 查询条件

                    rule_details_list = rule_details
                    rule_details_li = list(rule_details_list.split('and'))
                    print "rule_details_li_____________-", rule_details_li
                    """
                    [u'dport=78 ', u' sip=1.1.0.1 ', u' dip!=1.1.0.1 ', u' protocol=RDP ', u' ']
                    """

                    query_dict = {}
                    data_list = []
                    sql_str = ''
                    for i in rule_details_li:

                        if 'sip' in i:
                            if '!=' in i:
                                sip = i.split('!=')[1].replace(' ', '')
                                query_dict['sip'] = ['!=', sip]
                            else:
                                sip = i.split('=')[1].replace(' ', '')
                                query_dict['sip'] = ['=', sip]
                        if 'dip' in i:
                            if '!=' in i:
                                dip = i.split('!=')[1].replace(' ', '')
                                query_dict['dip'] = ['!=', dip]
                            else:
                                dip = i.split('=')[1].replace(' ', '')
                                query_dict['dip'] = ['=', dip]

                        if 'dport' in i:
                            dport = i.split('=')[1].replace(' ', '')
                            query_dict['dport'] = dport
                        if 'sport' in i:
                            sport = i.split('=')[1].replace(' ', '')
                            query_dict['sport'] = sport

                        if 'protocol' in i:
                            protocol = i.split('=')[1].replace(' ', '')
                            query_dict['protocol'] = protocol

                    print "query_dict-----------------", query_dict
                    for k, v in query_dict.items():

                        if k in ["sip", "dip"]:
                            if v[0] == '!=':
                                data = k + "!=" + "'%s'" % v[1]
                            else:
                                data = k + "=" + "'%s'" % v[1]
                        elif k in ["protocol"]:
                            data = k + "=" + "'%s'" % v
                        elif k in ["sport", "dport"]:
                            data = k + "=" + "%d" % int(v)
                        data_list.append(data)
                        # print "data_list---------", data_list
                    for dat in data_list:
                        sql_str += dat + " and "

                    print "print sql_str---------", sql_str
                    con = time_condition + " and " + sql_str

                    # 查询违规访问事件
                    sql1 = """select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id 
                            from internal_app_gxdx.illegal_access_event where %s """ % (con.rstrip("and "))
                    sql_data = "select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id from internal_app_gxdx.illegal_access_event " \
                               "where %s LIMIT %s OFFSET %s" % (
                                   con.rstrip("and "), page_size, from_index)

                    print "sql111,---------", sql1
                    json_data1 = json.loads(CFunction.execute(CPgSqlParam(sql1)))
                    if json_data1:
                        for item in json_data1:
                            result_dict1 = {}
                            result_dict1.update({"start_time": item[0], "end_time": item[1], "sip": item[2],
                                                 "sport": item[3], "dip": item[4], "dport": item[5],
                                                 "protocol": item[6],
                                                 "access_count": item[7], "real_id": item[8]})
                            result_list1.append(result_dict1)
                        total = len(result_list1)
                    else:
                        total = 0
                    json_data = json.loads(CFunction.execute(CPgSqlParam(sql_data)))

                    if json_data:
                        select_id = 1
                        for item in json_data:
                            result_dict = {}
                            result_dict.update(
                                {"id": select_id, "start_time": item[0], "end_time": item[1], "sip": item[2],
                                 "sport": item[3], "dip": item[4], "dport": item[5], "protocol": item[6],
                                 "access_count": item[7], "real_id": item[8]})
                            select_id += 1
                            result_list.append(result_dict)
                        result_list_dict.update({"results": result_list, "total": total})
                    else:
                        result_list_dict.update({"results": [], "total": 0})
                    return result_list_dict
                else:
                    # 自定义的规则为空;
                    # 查询违规访问事件
                    sql1 = """select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id 
                                                from internal_app_gxdx.illegal_access_event where %s """ % (
                        time_condition)
                    sql_data = "select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id from internal_app_gxdx.illegal_access_event " \
                               "where %s LIMIT %s OFFSET %s" % (
                                   time_condition, page_size, from_index)
                    print "sql1---------1111111111---------", sql1
                    json_data1 = json.loads(CFunction.execute(CPgSqlParam(sql1)))
                    if json_data1:
                        for item in json_data1:
                            result_dict1 = {}
                            result_dict1.update({"start_time": item[0], "end_time": item[1], "sip": item[2],
                                                 "sport": item[3], "dip": item[4], "dport": item[5],
                                                 "protocol": item[6],
                                                 "access_count": item[7], "real_id": item[8]})
                            result_list1.append(result_dict1)
                        total = len(result_list1)
                    else:
                        total = 0
                    print "sql_data================", sql_data, 111
                    json_data = json.loads(CFunction.execute(CPgSqlParam(sql_data)))

                    if json_data:
                        select_id = 1
                        for item in json_data:
                            result_dict = {}
                            result_dict.update(
                                {"id": select_id, "start_time": item[0], "end_time": item[1], "sip": item[2],
                                 "sport": item[3], "dip": item[4], "dport": item[5], "protocol": item[6],
                                 "access_count": item[7], "real_id": item[8]})
                            select_id += 1
                            result_list.append(result_dict)
                        result_list_dict.update({"results": result_list, "total": total})
                    else:
                        result_list_dict.update({"results": [], "total": 0})
                    return result_list_dict
        except Exception as e:
            logger.error("failed to get pgsql data")
            logger.error(str(e))
            return result_list_dict.update({"results": [], "total": 0})

    def get_rules_by_name(self, rule_name):
        """
        根据规则名称返回规则
        """
        try:
            rule_sql = """
            select rule_name, start_time, end_time, rule_details, protocol_dict from internal_app_gxdx.custom_rule   where rule_name = '%s'
            """ % rule_name
            print "sql----", rule_sql
            rule_sql_data = json.loads(CFunction.execute(CPgSqlParam(rule_sql)))
            if rule_sql_data:
                rule_name = rule_sql_data[0][0]
                start_time = rule_sql_data[0][1]
                end_time = rule_sql_data[0][2]
                rule_details = rule_sql_data[0][3]
                protocol_dict = rule_sql_data[0][4]
                print "--------------", rule_sql_data[0]
                rule_details_li = list(rule_details.split('and'))
                print "rule_details_li:", rule_details_li
                rule_dict = {}
                for i in rule_details_li:
                    if 'sip' in i:
                        if '!=' in i:
                            sip = i.split('=')[1].replace(' ', '')
                            rule_dict['sip'] = {"key": "not is", "value": sip}
                        else:
                            sip = i.split('=')[1].replace(' ', '')
                            rule_dict['sip'] = {"key": "is", "value": sip}

                    if 'dip' in i:
                        if '!=' in i:
                            dip = i.split('=')[1].replace(' ', '')
                            rule_dict['dip'] = {"key": "not is", "value": dip}
                        else:
                            dip = i.split('=')[1].replace(' ', '')
                            rule_dict['dip'] = {"key": "is", "value": dip}

                    if 'dport' in i:
                        dport = i.split('=')[1].replace(' ', '')
                        rule_dict['dport'] = dport

                    if 'sport' in i:
                        sport = i.split('=')[1].replace(' ', '')
                        rule_dict['sport'] = sport

                    if 'protocol' in i:
                        protocol = i.split('=')[1].replace(' ', '')
                        rule_dict['protocol'] = protocol

                rule_dict['rule_name'] = rule_name
                rule_dict['start_time'] = start_time
                rule_dict['end_time'] = end_time
                rule_dict['protocol_dict'] = protocol_dict

                print "query_dict-----------------", rule_dict, type(protocol_dict)

                if not rule_dict.has_key('protocol'):
                    rule_dict['protocol'] = ''
                if not rule_dict.has_key('sport'):
                    rule_dict['sport'] = ''
                if not rule_dict.has_key('dport'):
                    rule_dict['dport'] = ''
                if not rule_dict.has_key('dip'):
                    rule_dict['dip'] = {"key": "", "value": ''}
                if not rule_dict.has_key('sip'):
                    rule_dict['sip'] = {"key": "", "value": ''}
                return rule_dict
            else:
                return {"msg": "规则名称有误"}

        except Exception as e:
            logger.error("failed to get_rules")
            logger.error(str(e))

    def return_protocol(self):
        """
        返回协议给前端
        """
        try:
            protocol_map_list = []
            app_proto_map_list = []
            proto_maps_dict = {}
            protocol_map_dict = log_dict.protocol_map
            app_proto_map = log_dict.app_proto_map
            for k, v in protocol_map_dict.items():
                protocol_map_list.append(v)

            for k, v in app_proto_map.items():
                app_proto_map_list.append(v)
            # 合并列表
            proto_map = protocol_map_list + app_proto_map_list
            proto_maps = list(set(proto_map))
            k = 0
            for item in proto_maps:
                proto_maps_dict[k] = item
                k = k + 1
            return proto_maps_dict
        except Exception as e:
            logger.error(e.message)
            logger.error(traceback.format_exc())

    def get_events_by_name(self, rule_name):
        """
        展示违规访问事件列表:
        1.根据自定义规则的规则名称查询规则，
        2.根据规则查询违规事件
        :param params_dict:
        :return:
        """
        result_list1 = []
        result_list = []
        result_list_dict = {}

        try:
            if not rule_name:
                result_list_dict = {'msg': "自定义标签名不能为空"}
                return result_list_dict
            print "self.name:  ---", rule_name

            get_rule_sql = """
                select rule_details, start_time, end_time from internal_app_gxdx.custom_rule   where rule_name = '%s'  
            """ % rule_name
            get_rule_datas = json.loads(CFunction.execute(CPgSqlParam(get_rule_sql)))
            print "get_rule_datas------------", get_rule_datas
            if get_rule_datas:
                for item in get_rule_datas:
                    print "item ==============", item
                    rule_details = item[0]
                    start_ = time.localtime(int(item[1]))
                    print item[1], type(int(item[1]))

                    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", start_)
                    end_ = time.localtime(int(item[2]))
                    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", end_)
                time_condition = "start_time >='%s' and end_time<='%s'" % (start_time_str, end_time_str)
                print "time_condition-----------", time_condition
                if rule_details:
                    # 拼接 查询条件

                    rule_details_list = rule_details
                    rule_details_li = list(rule_details_list.split('and'))
                    print "rule_details_li_____________-", rule_details_li
                    """
                    [u'dport=78 ', u' sip=1.1.0.1 ', u' dip!=1.1.0.1 ', u' protocol=RDP ', u' ']
                    """

                    query_dict = {}
                    data_list = []
                    sql_str = ''
                    for i in rule_details_li:

                        if 'sip' in i:
                            if '!=' in i:
                                sip = i.split('!=')[1].replace(' ', '')
                                query_dict['sip'] = ['!=', sip]
                            else:
                                sip = i.split('=')[1].replace(' ', '')
                                query_dict['sip'] = ['=', sip]
                        if 'dip' in i:
                            if '!=' in i:
                                dip = i.split('!=')[1].replace(' ', '')
                                query_dict['dip'] = ['!=', dip]
                            else:
                                dip = i.split('=')[1].replace(' ', '')
                                query_dict['dip'] = ['=', dip]

                        if 'dport' in i:
                            dport = i.split('=')[1].replace(' ', '')
                            query_dict['dport'] = dport
                        if 'sport' in i:
                            sport = i.split('=')[1].replace(' ', '')
                            query_dict['sport'] = sport

                        if 'protocol' in i:
                            protocol = i.split('=')[1].replace(' ', '')
                            query_dict['protocol'] = protocol

                    print "query_dict-----------------", query_dict
                    for k, v in query_dict.items():

                        if k in ["sip", "dip"]:
                            if v[0] == '!=':
                                data = k + "!=" + "'%s'" % v[1]
                            else:
                                data = k + "=" + "'%s'" % v[1]
                        elif k in ["protocol"]:
                            data = k + "=" + "'%s'" % v
                        elif k in ["sport", "dport"]:
                            data = k + "=" + "%d" % int(v)
                        data_list.append(data)
                        # print "data_list---------", data_list
                    for dat in data_list:
                        sql_str += dat + " and "

                    print "print sql_str---------", sql_str
                    con = time_condition + " and " + sql_str

                    # 查询违规访问事件
                    sql1 = """select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id 
                            from internal_app_gxdx.illegal_access_event where %s """ % (con.rstrip("and "))
                    sql_data = "select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id from internal_app_gxdx.illegal_access_event " \
                               "where %s " % (
                                   con.rstrip("and "))

                    print "sql111,---------", sql1
                    json_data1 = json.loads(CFunction.execute(CPgSqlParam(sql1)))
                    if json_data1:
                        """
                        temp_dict = [result.get("id"),
                         result.get("start_time"),
                         result.get("end_time"),
                         result.get("sip"),
                         result.get("sport"),
                         result.get("dip"),
                         result.get("dport"),
                         result.get("protocol"),
                         result.get("access_count")]
            data_list1.append(temp_dict)
                        """

                        for item in json_data1:
                            result_dict1 = {}
                            result_dict1.update({"start_time": item[0], "end_time": item[1], "sip": item[2],
                                                 "sport": item[3], "dip": item[4], "dport": item[5],
                                                 "protocol": item[6],
                                                 "access_count": item[7], "real_id": item[8]})

                            result_list1.append(result_dict1)
                        total = len(result_list1)
                    else:
                        total = 0
                    json_data = json.loads(CFunction.execute(CPgSqlParam(sql_data)))

                    if json_data:
                        select_id = 1
                        for item in json_data:
                            result_dict = {}
                            result_dict.update(
                                {"id": select_id, "start_time": item[0], "end_time": item[1], "sip": item[2],
                                 "sport": item[3], "dip": item[4], "dport": item[5], "protocol": item[6],
                                 "access_count": item[7], "real_id": item[8]})

                            temp_dict = [select_id,
                                         item[0],
                                         item[1],
                                         item[2],
                                         item[3],
                                         item[4],
                                         item[5],
                                         item[6],
                                         item[7]]
                            select_id += 1
                            result_list.append(temp_dict)
                        return result_list
                    else:
                        return result_list
                else:
                    # 自定义的规则为空;
                    # 查询违规访问事件
                    sql1 = """select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id 
                                                from internal_app_gxdx.illegal_access_event where %s """ % (
                        time_condition)
                    sql_data = "select start_time::text,end_time::text,sip,sport,dip,dport,protocol,access_count ,id from internal_app_gxdx.illegal_access_event " \
                               "where %s " % (
                                   time_condition)
                    print "sql1---------1111111111---------", sql1
                    json_data1 = json.loads(CFunction.execute(CPgSqlParam(sql1)))
                    if json_data1:
                        for item in json_data1:
                            result_dict1 = {}
                            result_dict1.update({"start_time": item[0], "end_time": item[1], "sip": item[2],
                                                 "sport": item[3], "dip": item[4], "dport": item[5],
                                                 "protocol": item[6],
                                                 "access_count": item[7], "real_id": item[8]})
                            result_list1.append(result_dict1)
                        total = len(result_list1)
                    else:
                        total = 0
                    print "sql_data================", sql_data, 111
                    json_data = json.loads(CFunction.execute(CPgSqlParam(sql_data)))

                    if json_data:
                        select_id = 1
                        for item in json_data:
                            result_dict = {}
                            result_dict.update(
                                {"id": select_id, "start_time": item[0], "end_time": item[1], "sip": item[2],
                                 "sport": item[3], "dip": item[4], "dport": item[5], "protocol": item[6],
                                 "access_count": item[7], "real_id": item[8]})
                            temp_dict = [select_id,
                                         item[0],
                                         item[1],
                                         item[2],
                                         item[3],
                                         item[4],
                                         item[5],
                                         item[6],
                                         item[7]]
                            select_id += 1
                            result_list.append(temp_dict)
                        return result_list
                    else:
                        return result_list
        except Exception as e:
            logger.error("failed to get pgsql data")
            logger.error(str(e))


if __name__ == '__main__':
    dd = {
        "rule_name": "规则名称102",
        "start_time": 1630910674,
        "end_time": 1630910674,
        "sip": "10.0.0.1",
        "sport": '3304',
        "protocol": "RDP",
        "protocol_dict": {"id": 12, "name": "RDP"}

    }

    dd_dict = {
    "rule_name": "sdfssdf.",
    "start_time": 1635733363,
    "end_time": 1636078963,
    "sip": {
        "key": "",
        "value": ""
    },
    "sport": "",
    "dip": {
        "key": "",
        "value": ""
    },
    "dport": "",
    "protocol": "",
    "protocol_dict": {
        "id": 0,
        "name": ""
    }
}

    rule_name = "分分分"
    page_size = 10
    page_index = 1

    print IllegalEventRule().add_custome_rule(dd_dict)
    # print IllegalEventRule().delete_custom_rule(rule_name)
    # print IllegalEventRule().get_events_by_rules(rule_name, page_size, page_index)
    # print IllegalEventRule().get_rules_by_name("规则名称1017")
    # print IllegalEventRule().return_protocol()

    # ss = {"sip":"10.0.0.1"}

    # print IllegalEventRule().validate_conditions("sip","10.0.0.1")
