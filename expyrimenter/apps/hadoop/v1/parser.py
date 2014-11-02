#!/usr/bin/env python3
import sys
from pathlib import Path
import re
import logging
import glob


class Job:
    keys_fields = {
        'JOBID'        : 'id',
        'SUBMIT_TIME'  : 'submit',
        'LAUNCH_TIME'  : 'launch',
        'TOTAL_MAPS'   : 'maps',
        'TOTAL_REDUCES': 'reduces',
        'FINISH_TIME'  : 'finish',
    }

    def __init__(s, job_id):
        s.id = job_id
        s.submit = -1
        s.launch = -1
        s.maps = -1
        s.reduces = -1
        s.finish = -1
        s.tasks = []

    def __str__(s):
        string = "Job %s\n" % s.id
        string += "\tsubmitted: %d\n" % s.submit
        string += "\tlaunched : %d (%d)\n" % (s.launch, s.launch - s.submit)
        string += "\tfinished : %d (%d)\n" % (s.finish, s.finish - s.submit)
        string += "\tmaps     : %d\n" % s.maps
        string += "\treduces  : %d\n" % s.reduces
        string += "\ttasks    : %d\n" % len(s.tasks)
        string += "\tworkers  : %d\n" % len(s.hosts)

        return string

    @property
    def hosts(s):
        workers = set()
        for task in s.tasks:
            workers.add(task.host)
        return workers

    def map_tasks(s):
        return [t for t in s.tasks if t.type == 'MAP']

    def reduce_tasks(s):
        return [t for t in s.tasks if t.type == 'REDUCE']


class Task:
    keys_fields = {
        'TASKID'     : 'id',
        'TASK_TYPE'  : 'type',
        'TASK_STATUS': 'status',
        'START_TIME' : 'start',
        'FINISH_TIME': 'finish',
    }

    def __init__(s, task_id):
        s.id = task_id
        s.type = ''  # 'SETUP', 'MAP', 'REDUCE', 'CLEANUP'
        s.start = -1
        s.status = ''
        s.finish = -1
        s.attempts = {}
        s._succ_att = None

    @property
    def host(s):
        return s.successful_attempt.host

    @property
    def successful_attempt(s):
        if s._succ_att is None:
            atts = s.attempts.values()
            succs = [att for att in atts if att.status == 'SUCCESS']
            if len(succs) != 1:
                Parser.LOG.error('task %s has %d successful attempts' % (s.id,
                                                                         len(succs)))
            else:
                s._succ_att = succs[0]

        return s._succ_att

    @property
    def attempt_start(s):
        return s.successful_attempt.start

    @property
    def attempt_finish(s):
        return s.successful_attempt.finish

    def __str__(s):
        string = "Task %s\n" % s.id
        string += "\tType    : %s\n" % s.type
        string += "\tstarted : %d\n" % s.start
        string += "\tstatus  : %s\n" % s.status
        string += "\tfinished: %d (%d)\n" % (s.finish, s.finish - s.start)
        string += "\thosts   : %s\n" % len(s.host)

        return string

    def __repr__(s):
        return s.__str__()


class Attempt:
    keys_fields = {
        'TASK_ATTEMPT_ID': 'id',
        'TASK_STATUS': 'status',
        'START_TIME': 'start',
        'FINISH_TIME': 'finish',
        'HOSTNAME': 'host',  # note: KILLED attempts do not include rack name
        'TASKID': None,
    }

    def __init__(s, attempt_id):
        s.id = attempt_id
        s.status = ''  # 'SUCCESS', 'KILLED'
        s.start = -1
        s.finish = -1
        s.host = ''

    def __str__(s):
        string = "Attempt %s\n" % s.id
        string += "\tstatus  : %s\n" % s.status
        string += "\tstarted : %d\n" % s.start
        string += "\tfinished: %d (%d)\n" % (s.finish, s.finish - s.start)
        string += "\thost    : %s\n" % s.host

        return string


class Parser:
    LOG = logging.getLogger('parser')

    def __init__(s):
        s.job = None
        s._tasks = None
        s._re_is_job = re.compile('^Job')
        s._re_is_task = re.compile('^Task')
        s._re_is_attempt = re.compile('^(Map|Reduce)Attempt')
        s._re_skip = re.compile('^Meta ')
        s._re_l2d = re.compile(r' (\w+?)="(.+?)"')

    def parse_files(s, shell_pattern):
        jobs = []
        for filename in glob.glob(shell_pattern):
            s._tasks = {}
            job = s.parse_file(filename)
            job.tasks = sorted(s._tasks.values(), key=lambda t: t.start)
            jobs.append(job)
        return sorted(jobs, key=lambda j: j.submit)

    def parse_file(s, job_file):
        log_file = Path(job_file)
        s.job = None
        with log_file.open() as f:
            for line in f:
                s._parse_line(line)
        return s.job

    def _parse_line(s, line):
        if s._re_is_job.match(line):
            s._parse_job(line)
        elif s._re_is_task.match(line):
            s._parse_task(line)
        elif s._re_is_attempt.match(line):
            s._parse_attempt(line)
        elif not s._re_skip.match(line):
            Parser.LOG.warn('Line not recognized: %s' % line)

    def _parse_job(s, line):
        fields_values = s._line2dict(line, Job.keys_fields)
        if s.job is None:
            job_id = fields_values['id']
            s.job = Job(job_id)
        s._assign2obj(s.job, fields_values)

    def _parse_task(s, line):
        fields_values = s._line2dict(line, Task.keys_fields)
        task_id = fields_values['id']
        task = s._get_task(task_id)
        s._assign2obj(task, fields_values)

    def _parse_attempt(s, line):
        fields_values = s._line2dict(line, Attempt.keys_fields)
        task_id = fields_values['TASKID']
        task = s._get_task(task_id)
        att_id = fields_values['id']
        attempt = task.attempts.setdefault(att_id, Attempt(att_id))
        s._assign2obj(attempt, fields_values)

    def _get_task(s, task_id):
        return s._tasks.setdefault(task_id, Task(task_id))

    def _line2dict(s, line, wanted_key_field):
        d = {}
        for kv in s._re_l2d.finditer(line):
            key = kv.group(1)
            if key in wanted_key_field:
                if wanted_key_field[key] is None:
                    field = key
                else:
                    field = wanted_key_field[key]
                value = kv.group(2)
                d[field] = value
        return d

    def _assign2obj(s, obj, fields_values):
        for field, str_value in fields_values.items():
            if field != 'id':
                try:
                    obj_field = getattr(obj, field)
                    if type(obj_field) == int:
                        value = int(str_value)
                    else:
                        value = str_value
                    setattr(obj, field, value)
                except AttributeError:
                    pass


def main():
    if len(sys.argv) < 2:
        print('Usage: %s log_file' % sys.argv[0])
        sys.exit(1)
    else:
        parser = Parser()
        job = parser.parse_files(sys.argv[1])
        print(job)

if __name__ == "__main__":
    main()
