# coding=utf-8
import time
import sys
import traceback
import json
import datetime
from dataInterface.functions import CFunction
from dataInterface.db.params import CPgSqlParam
from exportReport.report.task_tools import execute_report_task
from isopGxdx.utils.common import getLogger
from task_manager.taskmanager import TaskAction
ta = TaskAction.TaskAction()
logger = getLogger('report_task_model.log')


class ReportTaskModel(object):
    """
    报表任务 model
    """

    def __init__(self):
        pass

    def do_report(self, report_params, start_time, end_time, ip):
        """
        向报表引擎发送任务
        :return:
        """
        try:
            # _type =operation_report
            _type = report_params.pop("template_name")

            # 包边的名称
            if _type == "illegal_access_report":
                title = "违规访问报表"
            else:
                title = "XXX"
            report_params['_type'] = _type
            report_params['app_ch_name'] = '广西电信安全运营平台'
            app_name = 'isopGxdx'
            report_params['app_name'] = app_name
            report_params['title'] = title
            # 权限控制信息 非空
            region_id = report_params["region_id"]
            region_name = self.getRegionName(region_id)
            report_params['region'] = {'id': region_id, 'name': region_name}

            res = self.get_illegal_ip_list(start_time, end_time, ip)
            if res:
                logger.info("-----res--------2021-1--11-{}".format(res))
                report_id = execute_report_task(_type, report_params, app_name)

                if report_id:
                    logger.debug('【开始】报表请求参数：{}，生成报表任务成功,提交报表返回的ID：{}'.format(report_params, report_id))
                    return True

            logger.error("提交报表任务失败")
            return False
        except Exception as e:
            logger.exception("提交报表任务失败:%s" % e)
            return False

    def getRegionName(self, region_id):
        region_name = ""
        try:
            sql = " select name from internal_app_permission.tb_region where id = %s "
            rtn = CFunction.execute(CPgSqlParam(sql, params=(region_id,), dataFormat=None))
            if rtn:
                region_name = rtn[0][0]
                return region_name
        except Exception, e:
            logger.error(traceback.print_exc())
            return region_name

    def get_illegal_ip_list(self, start_time, end_time, ip):
        """获取ip列表"""
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time)))
        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(end_time)))

        time_query_sql = """ {db_end_time} >= '{start_time}' AND {db_start_time} <= '{end_time}' """.format(
            db_end_time="end_time", db_start_time="start_time", start_time=start_time,
            end_time=end_time
        )
        base_sql = """ SELECT {field} FROM internal_app_gxdx.illegal_access_event WHERE {time_query_sql}""".format(
            field=",".join(["sip", "sport", "dip", "dport", "protocol"]),
            time_query_sql=time_query_sql
        )
        if ip:
            ip_list = ip.split("-")
            ip_query_sql = """ AND dip >= '{}' AND dip <= '{}' """.format(
                ip_list[0], ip_list[1]
            )
            base_sql += ip_query_sql
        base_sql += " ORDER BY start_time desc "
        logger.debug(base_sql)
        print "base_sql---", base_sql
        res = CFunction.execute(CPgSqlParam(base_sql, "default", None))
        logger.debug("ip_list:{}".format(res))
        if not res:
            return False
        return True


class ReportTaskModelTime(object):

    def __init__(self):
        sql_time = datetime.datetime.now()
        self.end_time = sql_time.strftime("%Y-%m-%d %H:%M:%S")
        self.start_time = (
                sql_time - datetime.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("start_time111：{}".format(self.start_time))

    def do_report_time(self, report_params, rule_name):
        """
        定时向报表引擎发送任务
        :return:
        """
        try:
            logger.info("report_params---11：{}".format(report_params))
            # _type =operation_report
            _type = report_params.pop("template_name")
            reportName = report_params.get('reportName')
            task_name = report_params.get('task_name')
            # 包边的名称
            if _type == "timing_access_report":
                title = task_name + "_" + reportName
            else:
                title = "XXX"
            report_params['_type'] = _type
            report_params['app_ch_name'] = '广西电信安全运营平台'
            app_name = 'isopGxdx'
            report_params['app_name'] = app_name
            report_params['title'] = title
            zhouqi = report_params.get('zhouqi')
            zhouqi = int(zhouqi)

            # 权限控制信息 非空
            region_id = report_params["region_id"]
            region_name = self.getRegionName(region_id)
            report_params['region'] = {'id': region_id, 'name': region_name}
            res = self.get_events_by_rules(rule_name, zhouqi)
            logger.info("report_params---102----{}----".format(report_params))
            logger.info("res---102----{}----".format(res))
            if res:
                report_id = execute_report_task(_type, report_params, app_name)
                logger.info("report_id--------------{}".format(report_id))
                if report_id:
                    logger.debug('【开始】报表请求参数：{}，生成报表任务成功,提交报表返回的ID：{}'.format(report_params, report_id))
                    return True

            logger.error("提交报表任务失败")
            return False
        except Exception as e:
            logger.exception("提交报表任务失败:%s" % e)
            return False

    def getRegionName(self, region_id):
        region_name = ""
        try:
            sql = " select name from internal_app_permission.tb_region where id = %s "
            rtn = CFunction.execute(CPgSqlParam(sql, params=(region_id,), dataFormat=None))
            if rtn:
                region_name = rtn[0][0]
                return region_name
        except Exception, e:
            logger.error(traceback.print_exc())
            return region_name

    def get_events_by_rules(self, rule_name, zhouqi):
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
                    # start_ = time.localtime(int(item[1]))
                    # 
                    # start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", start_)
                    # end_ = time.localtime(int(item[2]))
                    # end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", end_)
                cur_str_time = datetime.datetime.now()  # 当前时间
                cur_time = cur_str_time.strftime("%Y-%m-%d %H:%M:%S")
                zhouqi_time = (cur_str_time + datetime.timedelta(days =- int(zhouqi))).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("--rule_name-----{}".format(rule_name))

                if rule_details and (cur_time and zhouqi_time):
                    # 拼接 查询条件
                    time_condition = "start_time >='%s' and end_time<='%s'" % (zhouqi_time, cur_time)
                    logger.info("--time_condition----{}".format(time_condition))
                    print "time_condition-----------", time_condition
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
                    sql1 = """select sip, dip, dport, protocol, count(*) 
                            from internal_app_gxdx.illegal_access_event where %s  group by sip, dip, dport, protocol 
                            """ % (con.rstrip("and "))
                    logger.info("--sql1, --sql1-----{}".format(sql1))
                    json_data = json.loads(CFunction.execute(CPgSqlParam(sql1)))
                    if json_data:
                        return True
                    else:
                        return False
                elif (cur_time and zhouqi_time):
                    time_condition = "start_time >='%s' and end_time<='%s'" % (zhouqi_time, cur_time)
                    print "time_condition-----------", time_condition

                    # 查询违规访问事件
                    sql1 = """select sip, dip, dport, protocol, count(*) 
                                                from internal_app_gxdx.illegal_access_event where %s  group by sip, dip, dport, protocol 
                                                """ % (time_condition)
                    json_data = json.loads(CFunction.execute(CPgSqlParam(sql1)))
                    if json_data:
                        return True
                    else:
                        return False

        except Exception as e:
            logger.error("failed to get pgsql data")
            logger.error(str(e))

    def get_report_params(self, task_name):
        """
        开启定时任务： 获取前端传来的参数time_params
        获取报表传来的参数
        """

        try:
            logger.info("--report_p---task_name----{}".format(task_name))
            get_sql = """
                                    select time_params from internal_app_gxdx.illegal_rule_group where task_name = '%s'
                                """ % task_name
            print "get_sql--------", get_sql
            get_sql_data = json.loads(CFunction.execute(CPgSqlParam(get_sql)))
            print "get_sql_data", get_sql_data
            if get_sql_data:
                report_params = get_sql_data[0][0]
                logger.info("--report_params--------{}".format(report_params))
            else:
                report_params = {}
            return report_params
        except Exception as e:
            logger.error("failed to get  data")
            logger.error(str(e))

    def excute_timing_task_by_time(self, task_name_params):
        """
        判断当前时间 是否在开始时间和结束时间之间
        """
        # 获取系统当前时间戳
        task_name = task_name_params
        current_time = int(time.time())
        start_time = 0
        end_time = 0
        zhouqi = 0
        try:
            # 更新定时任务
            job_param = {
                "task_name": "report_task_model",
                "task_type": 1,
                "exec_cmd": "python /home/master/ISOP/apps/isopGxdx/models/report_task_model.pyc",
                "task_owner": "ISOP",
                "run_mode": 1,
                "duration_args": "0 0 * * * ?",
                "retry_nums": 3,
                "task_description": "根据周期定时输出报表",
                "is_enable": 1
            }
            
            # 获取start_time， end_time
            time_sql = """
            select start_time, end_time, timing, rule_name from internal_app_gxdx.illegal_rule_group where task_name = '%s'
            """ % task_name
            times = json.loads(CFunction.execute(CPgSqlParam(time_sql)))
            print "time_sql----",time_sql
            if times:
                start_time = times[0][0]
                end_time = times[0][1]
                zhouqi = times[0][2]
                rule_name = times[0][3]
                logger.info("--start_time, --end_time-----{},{},{}".format(start_time, end_time, zhouqi))
                logger.info("-task_name-----{}".format(task_name))
                report_params = self.get_report_params(task_name)

                cur_str_time = int(time.time())  # 当前时间
                
                start_time = cur_str_time - int(zhouqi) * 24 * 3600
                report_params['end_time'] = cur_str_time
                report_params['start_time'] = start_time
                logger.info("--start_time, --end_time-----{},{}".format(start_time,end_time))

                if current_time < int(end_time):
                    # 当前日期在选择的时间范围之内，执行报表任务
                    logger.info("--report_params, --report_params-----{},{},{}".format(report_params, report_params, zhouqi))
                    res = self.do_report_time(report_params, rule_name)
                    logger.info("--task_name----{}".format(task_name))
                    job_param["task_name"] = task_name
                    # job_param["is_enable"] = 1
                    # 定时任务
                    exec_cmd = "python /home/master/ISOP/apps/isopGxdx/models/report_task_model.pyc %s" % task_name_params
                    job_param["exec_cmd"] = exec_cmd
                    # duration_args = " 0 */%s * * * ?" % int(zhouqi)  # 单位是分钟
                    # duration_args = " 0 0 1 /%s * ?" % int(zhouqi)  # 单位是天   0 0 0 */3 * ?   0 0 0 1/3 * ?
                    duration_args = "0 0 0 1/%s * ?" % int(zhouqi)  # 单位是天
                    job_param["duration_args"] = duration_args
                    ta.taskUpdate(job_param)
                    ta.taskRestart(job_param)
                    logger.info("--job_param, 234----{}".format(job_param))
                else:
                    res = {"msg": "当前时间不在范围之内"}
            else:
                return {"msg": "开启定时任务失败"}
        except Exception as e:
            logger.exception("提交报表任务失败:%s" % e)
            return False
        return res

    def get_task_name(self):
        """
        获取定时任务的name
        """
        task_name_list = []
        try:
            get_sql = """
                        select task_name from internal_app_gxdx.illegal_rule_group
                    """

            get_sql_data = json.loads(CFunction.execute(CPgSqlParam(get_sql)))
            if get_sql_data:
                for item in get_sql_data:
                    task_name = item[0]
                    task_name_list.append(task_name)
            else:
                task_name_list = []
            return task_name_list
        except Exception as e:
            logger.error("failed to get  task_name")
            logger.error(str(e))

    def cal_time(self, a, b):
        t1 = time.localtime(a)
        t2 = time.localtime(b)
        t1 = time.strftime("%Y-%m-%d %H:%M:%S", t1)
        t2 = time.strftime("%Y-%m-%d %H:%M:%S", t2)
        time1 = datetime.datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
        time2 = datetime.datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")
        return int((time2 - time1).days)


if __name__ == '__main__':

    # ReportTaskModel().do_report(report_params, ip, start_time, end_time)
    # print report_params, rule_name
    # 接收 定时任务的参数(rule_name)

    rt = ReportTaskModelTime().get_task_name()
    args = sys.argv
    logger.info("--args, --args-----{}".format(args))
    if args[1] in rt:
        logger.info("--args[1], --args[1]-----{}".format(args[1]))
    #     将任务名称 args[1] 传到定时任务里面
        # pass
        ReportTaskModelTime().excute_timing_task_by_time(args[1])
    # print ReportTaskModelTime().get_report_params()
    # print ReportTaskModelTime().get_events_by_rules("规则名称005")
    # ReportTaskModelTime().excute_timing_task_by_time("规则名称005")







