# coding=utf-8
# date:2021-09-27
# zzs

import time
import os
import datetime
import traceback
import json
from dataInterface.functions import CFunction
from dataInterface.db.params import CPgSqlParam
from isopGxdx.utils.common import getLogger,APPHOME
from isopGxdx.utils import IniParser

from isopGxdx.models.report_task_model import ReportTaskModelTime
from permissionmanage.cur_models.accountmodels import User
from task_manager.taskmanager import TaskAction

ta = TaskAction.TaskAction()
user = User()

logger = getLogger('timing_model.log')

report_task_model_time = ReportTaskModelTime()


class TimingModel(object):
    """
    定时任务
    """
    def __init__(self):
        self.conf_path = os.path.join(APPHOME, "conf/start_nums.conf")
        self.conf = IniParser(self.conf_path)

    def get_task_name(self):
        """
        获取表中的定时任务名称
        """
        try:
            sql = """
            select task_name from internal_app_gxdx.illegal_rule_group 
            """
            task_names = json.loads(CFunction.execute(CPgSqlParam(sql)))
            task_names_list = []
            if task_names:
                for item in task_names:
                    task_names_list.append(item)
                return task_names_list
            else:
                return task_names_list

        except Exception as e:
            logger.error(e.message)
            logger.error(traceback.format_exc())

    def timing_task_model(self, timing_params):
        """
        根据时间范围和周期定时执行report_task_model.py 脚本
        """

        try:
            dat = {}
            job_param = {
                "task_name": "report_task_model",
                "task_type": 1,
                "exec_cmd": "python /home/master/ISOP/apps/isopGxdx/models/report_task_model.pyc",
                "task_owner": "ISOP",
                "run_mode": 1,
                "duration_args": "0 0 * * * ?",
                "retry_nums": 3,
                "task_description": "根据周期定时输出报表",
                "is_enable": 0
            }
            is_start = timing_params['is_start']
            task_name = timing_params['task_name']  # 获取前端传来的任务名称
            start_time = timing_params.get("start_time", None)
            end_time = timing_params.get("end_time", None)

            print "timing_params---------------", timing_params
            start_nums = self.conf.get('start_files', 'start_numbers')
            starts = int(start_nums)
            if is_start:  # 启用
                # 不能开启8个以上的定时任务
                start_sql = """ select count(*) from internal_app_gxdx.illegal_rule_group where flag = 1
                                """
                start_num = json.loads(CFunction.execute(CPgSqlParam(start_sql)))
                if start_num[0][0] >= starts:
                    dat['msg'] = "不能开启%s个以上的定时任务" % starts
                    dat['result'] = False
                    logger.info("不能开启8个以上的定时任务------{}".format(job_param))
                    return dat
                # 如果当前时间大于定时任务的结束时间，点击开启按钮，提示时间已过期，无法开启定时任务
                if int(time.time()) >= int(end_time):
                    dat['msg'] = "时间已过期，无法开启定时任务"
                    dat['result'] = False
                    logger.info("时间已过期，无法开启定时任务------{}".format(job_param))
                    return dat
                # 如果当前时间大于定时任务的结束时间，点击开启按钮，提示时间已过期，无法开启定时任务
                # if int(time.time()) >= int(end_time):
                #     dat['msg'] = "时间已过期，无法开启定时任务"
                #     dat['result'] = False
                #     logger.info("时间已过期，无法开启定时任务------{}".format(job_param))
                #     return dat

                sql = """ select flag, task_times, start_time, timing from internal_app_gxdx.illegal_rule_group where task_name = '%s'
                """ % task_name
                flag_data = json.loads(CFunction.execute(CPgSqlParam(sql)))
                logger.info("is_start--{},{}".format(is_start, is_start))
                task_times = int(flag_data[0][1])
                start_time = int(flag_data[0][2])
                timing = int(flag_data[0][3])
                if flag_data and int(flag_data[0][0]) == 0:
                    # 开启定时任务---> 更新 is_enable
                    update_sql = """ update internal_app_gxdx.illegal_rule_group set flag = %d  where task_name = '%s'
                                    """ % (is_start, task_name)
                    CFunction.execute(CPgSqlParam(update_sql))
                    
                    if int(time.time()) > int(start_time):
                        logger.info("start_time--11,..{},{}".format(start_time, start_time))
                        zhouqi_time_range = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time()) + 100))
                        logger.info("start_time--31,..{},{}".format(start_time, start_time))
                    else:
                        logger.info("start_time--11,..{},{}".format(start_time, start_time))
                        zhouqi_time_range = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time) + 100))
                        logger.info("start_time-2-1-{},{}".format(is_start, is_start))
                    zhouqi_time_range = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time)))
                    month = zhouqi_time_range[5:7]
                    days_time = zhouqi_time_range[8:10]
                    hour_time = zhouqi_time_range[11:13]
                    minute_time = zhouqi_time_range[14:16]
                    duration_args = " 0 %s %s %s %s ?" % (
                        int(minute_time), int(hour_time), int(days_time),int(month))
                    exec_cmd = "python /home/master/ISOP/apps/isopGxdx/models/report_task_model.pyc %s" % task_name
                    logger.info("job_param-111---update--job_param------{}".format(job_param))
                    job_param["is_enable"] = 1
                    job_param["task_name"] = task_name
                    job_param["duration_args"] = duration_args
                    job_param["exec_cmd"] = exec_cmd
                    if task_times:
                        ta.taskUpdate(job_param)
                        logger.info("job_param-0000---0------{}".format(job_param))
                        logger.info("job_param-task_times-----{}".format(task_times))
                    else:
                        ta.taskAdd(job_param)
                        update_times_sql = """
                            update internal_app_gxdx.illegal_rule_group set task_times = 1 where task_name = '%s'
                            """ % task_name
                        CFunction.execute(CPgSqlParam(update_times_sql))
                        logger.info("job_param-task_times--111---{}".format(task_times))
                    ta.taskRestart(job_param)
                    dat['msg'] = "开启成功！！！"
                    dat['result'] = True
                    logger.info("job_param-111-222--start--job_param------{}".format(job_param))
                    return dat
                
                # elif flag_data and int(flag_data[0][0]) == 1:
                #     # 注册定时任务
                #     zhouqi_time_range = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time + 60)))
                #     month = zhouqi_time_range[5:7]
                #     days_time = zhouqi_time_range[8:10]
                #     hour_time = zhouqi_time_range[11:13]
                #     minute_time = zhouqi_time_range[14:16]
                #     logger.info("timing_param--------------,{}".format(123123))
                #     zhouqi = timing_params['zhouqi']
                #     rule_name = timing_params['rule_name']
                #     # 添加定时任务
                #     try:
                #         job_param["is_enable"] = 1
                #         duration_args = " 0 %s %s %s %s ?" % (int(minute_time), int(hour_time), int(days_time), int(month))
                #         exec_cmd = "python /home/master/ISOP/apps/isopGxdx/models/report_task_model.pyc %s" % task_name
                #         logger.info("job_param-111--job_param------{}".format(job_param))
                #         job_param["task_name"] = task_name
                #         job_param["duration_args"] = duration_args
                #         job_param["exec_cmd"] = exec_cmd
                #
                #         ta.taskAdd(job_param)
                #         ta.taskRestart(job_param)
                #         logger.info("ta-------ta-------234,{}".format(ta))
                #         dat['msg'] = "timing start success"
                #         dat['result'] = True
                #         logger.info("job_param---job_param------{}".format(job_param))
                #         logger.info("timing_params-------dat------------------0123--{},{}".format(timing_params, dat))
                #         logger.info("timing_params-------dat-------234,{}".format(duration_args))
                #
                #     except Exception as e:
                #         logger.exception(e)
                #         dat['msg'] = "定时任务执行失败"
                #         dat['result'] = False
                #     return dat
                elif flag_data and int(flag_data[0][0]) == 1:
                    dat['msg'] = "该任务已经开启了！！！"
                    dat['result'] = False
                    logger.info("job_param-111-222--update--job_param------{}".format(job_param))
                    return dat

            else:
                # 关闭定时任务
                try:
                    job_param["task_name"] = task_name
                    job_param["is_enable"] = 0
                    ta.taskUpdate(job_param)
                    ta.taskStop(job_param)
                    # ta.taskDelete(job_param)
                    # 更新illegal_rule_group 库表中的定时任务
                    update_sql = """
                    update internal_app_gxdx.illegal_rule_group set flag = %d where task_name = '%s'
                    """ % (is_start, task_name)
                    CFunction.execute(CPgSqlParam(update_sql))
                    dat['msg'] = "定时任务已经关闭"
                    dat['result'] = True
                    return dat
                except Exception as e:
                    logger.exception(e)

        except Exception as e:
            logger.error(e.message)
            logger.error(traceback.format_exc())

    def add_timing_data(self, paramas_dict, params):
        """
        1. 将周期， 时间范围，规则name, flag(是否启用标志), 定时参数，保存到 internal_app_gxdx.illegal_rule_group
        """
        result = {}
        dat = {}
        try:
            # 获取系统当前时间戳
            cur_time = int(time.time())
            logger.info(paramas_dict)
            zhouqi = paramas_dict.get('zhouqi')
            start_time = paramas_dict.get('start_time')
            end_time = paramas_dict.get('end_time')
            # flag：是否启用定时， 0： 禁用， 1：启用
            is_start = paramas_dict.get('is_start')
            rule_name = paramas_dict.get('rule_name', '')
            task_name = paramas_dict.get('task_name', '')
            # day_remain = int(end_time - start_time)
            day_remain = self.cal_time(int(start_time), int(end_time))
            logger.info("paramas_dict-------{}".format(paramas_dict))

            if int(end_time) < int(start_time):
                result = {"msg": u"开始时间不能大于结束时间，请重新选择时间范围", "ret": False}
                return result
            if cur_time > int(start_time + 600):
                result = {"msg": u"当前时间不能大于开始时间，请重新选择时间范围", "ret": False}
                return result
            if int(zhouqi) > day_remain:
                result = {"msg": u"周期不能大于时间范围", "ret": False}
                return result
            if not rule_name:
                result = {"msg": u"规则名称不能为空", "ret": False}
                return result
            if (not task_name) or len(task_name) > 20:
                result = {"msg": u"任务名称不能为空且不能超过20位", "ret": False}
                return result

            re_string = "~!@#$%^&*()+*/<>,[]\/ 。，;；？?{}【】"
            rule_name_list = list(task_name)
            re_string_list = list(re_string)
            tmp = [val for val in rule_name_list if val in re_string_list]
            if tmp:
                result = {"msg": u"任务名称不能包含特殊字符", "ret": False}
                return result

            is_rule_name = """
            select rule_name from internal_app_gxdx.custom_rule
            """
            rule_names_list = []
            rule_names = json.loads(CFunction.execute(CPgSqlParam(is_rule_name)))
            if rule_names:
                for item in rule_names:
                    rule_names_list.append(item[0])
            logger.info("rule_names_list-------{}".format(rule_names_list))
            if rule_name not in rule_names_list:
                logger.info("rule_names_list---11----{}".format(rule_names_list))
                result = {"msg": u"创建失败，自定义规则不存在！", "ret": False}
                return result

            # 判断定时任务是否存在， 如果存在，提示不能重复添加定时任务，否则添加定时任务
            task_names_list = self.get_task_name()
            if task_name in task_names_list:
                logger.info("timing_data-------{}".format(task_name))
                dat['ret'] = False
                dat['msg'] = u"定时任务已存在,不能重复添加定时任务"
                return dat

            # if len(task_names_list) > 7:
            #     dat['ret'] = False
            #     dat['msg'] = "添加的定时任务不能超过8个,请先关闭开启的定时任务"
            #     return dat

            # 判断定时任务是否重复
            timing_sql = """
                select task_name from internal_app_gxdx.illegal_rule_group where task_name = '%s'
            """ % task_name
            timing_data = json.loads(CFunction.execute(CPgSqlParam(timing_sql)))
            if timing_data:
                logger.info("timing_data-------{}".format(timing_data))
                result = {"msg": u"定时任务名称不能重复添加", "ret": False}
                return result

            # 将规则名称和周期保存pg中
            logger.info((is_start))
            logger.info((zhouqi))
            task_times = 0
            sql = """
                INSERT INTO internal_app_gxdx.illegal_rule_group
                    (rule_name, timing, start_time, end_time, flag, time_params, task_name, task_times) 
                VALUES ('%s', %d, '%s', '%s', %d, '%s', '%s', %d)
                """ % (rule_name, int(zhouqi), start_time, end_time, int(is_start),
                       json.dumps(params), task_name, task_times)
            CFunction.execute(CPgSqlParam(sql))
            result = {"msg": "开启成功", "ret": True}
            return result

        except Exception as e:
            logger.error(e.message)
            logger.error(traceback.format_exc())

    def cal_time(self, a, b):
        t1 = time.localtime(a)
        t2 = time.localtime(b)
        t1 = time.strftime("%Y-%m-%d %H:%M:%S", t1)
        t2 = time.strftime("%Y-%m-%d %H:%M:%S", t2)
        time1 = datetime.datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
        time2 = datetime.datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")
        return int((time2 - time1).days)
    
    def delete_task(self, task_name):
        """
        删除定时任务：
        """
        try:
            dat = {}
            if not task_name:
                dat['msg'] = "任务不能为空"
                dat['result'] = False
                return dat

            job_param = {
                "task_name": "report_task_model",
                "task_type": 1,
                "exec_cmd": "python /home/master/ISOP/apps/isopGxdx/models/report_task_model.pyc",
                "task_owner": "ISOP",
                "run_mode": 1,
                "duration_args": "0 0 * * * ?",
                "retry_nums": 3,
                "task_description": "根据周期定时输出报表",
                "is_enable": 0
            }
            
            job_param["task_name"] = task_name
            job_param["is_enable"] = 0
            ta.taskUpdate(job_param)
            ta.taskStop(job_param)
            ta.taskDelete(job_param)
            # 删除illegal_rule_group 库表中的定时任务
            delete_sql = """
                                delete from internal_app_gxdx.illegal_rule_group where task_name = '%s'
                                """ % task_name
            CFunction.execute(CPgSqlParam(delete_sql))
            dat['msg'] = "定时任务已经删除"
            dat['result'] = True
            return dat
        except Exception as e:
            logger.error(e.message)
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    paramas_dict = {
        "is_start": 1,
        "task_name":"task002"
    }
    params = {
        "template_name": "illegal_access_report",
        "region_id": 0,
        "_type": "illegal_access_report",
        "app_ch_name": "广西电信安全运营平台",
        "app_name": "isopGxdx",
        "title": "违规访问报表",

        "region": {'id': 0, 'name': "全局数据域"},
        "start_time": 1633416274,
        "end_time": 1633848274,

    }
    rule_name = "分分分"
    # print TimingModel().add_timing_data(paramas_dict, params)

    print TimingModel().timing_task_model(paramas_dict)
    # print TimingModel().delete_job()





