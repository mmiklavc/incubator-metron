#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
import re
import subprocess
import time

from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import Template
from resource_management.libraries.functions import format


# Wrap major operations and functionality in this class
class Commands:
    __params = None
    __parser_list = None
    __configured = False

    def __init__(self, params):
        if params is None:
            raise ValueError("params argument is required for initialization")
        self.__params = params
        self.__parser_list = self.__get_parsers(params)
        self.__configured = os.path.isfile(self.__params.configured_flag_file)

    # get list of parsers
    def __get_parsers(self, params):
        return params.parsers.replace(' ', '').split(',')

    def is_configured(self):
        return self.__configured

    def set_configured(self):
        File(self.__params.configured_flag_file,
             content="",
             owner=self.__params.metron_user,
             mode=0775)

    # Possible storm topology status states
    # http://storm.apache.org/releases/0.10.0/javadocs/backtype/storm/generated/TopologyStatus.html
    class StormStatus:
        ACTIVE = "ACTIVE"
        INACTIVE = "INACTIVE"
        KILLED = "KILLED"
        REBALANCING = "REBALANCING"

    def init_parsers(self):
        Logger.info(
            "Copying grok patterns from local directory '{}' to HDFS '{}'".format(self.__params.local_grok_patterns_dir,
                                                                                  self.__params.metron_apps_dir))
        self.__params.HdfsResource(self.__params.metron_apps_dir,
                                   type="directory",
                                   action="create_on_execute",
                                   owner=self.__params.metron_user,
                                   mode=0775,
                                   source=self.__params.local_grok_patterns_dir)

        Logger.info("Creating global.json file")
        File(self.__params.metron_zookeeper_config_path + '/global.json',
             content=Template("metron-global.json"),
             owner=self.__params.metron_user,
             mode=0775)
        Logger.info("Done initializing parser configuration")

    def get_parser_list(self):
        return self.__parser_list

    def setup_repo(self):
        def local_repo():
            Logger.info("Setting up local repo")
            Execute("yum -y install createrepo")
            Execute("createrepo /localrepo")
            Execute("chmod -R o-w+r /localrepo")
            Execute("echo \"[METRON-0.2.0BETA]\n"
                    "name=Metron 0.2.0BETA packages\n"
                    "baseurl=file:///localrepo\n"
                    "gpgcheck=0\n"
                    "enabled=1\" > /etc/yum.repos.d/local.repo")

        def remote_repo():
            print('Using remote repo')

        yum_repo_types = {
            'local': local_repo,
            'remote': remote_repo
        }
        repo_type = self.__params.yum_repo_type
        if repo_type in yum_repo_types:
            yum_repo_types[repo_type]()
        else:
            raise ValueError("Unsupported repo type '{}'".format(repo_type))

    def init_kafka_topics(self):
        Logger.info('Creating Kafka topics')
        command_template = """{}/kafka-topics.sh \
                                --zookeeper {} \
                                --create \
                                --topic {} \
                                --partitions {} \
                                --replication-factor {} \
                                --config retention.bytes={}"""
        num_partitions = 1
        replication_factor = 1
        retention_gigabytes = 10
        retention_bytes = retention_gigabytes * 1024 * 1024 * 1024
        Logger.info("Creating main topics for parsers")
        for parser_name in self.get_parser_list():
            Logger.info("Creating topic'{}'".format(parser_name))
            Execute(command_template.format(self.__params.kafka_bin_dir,
                                            self.__params.zookeeper_quorum,
                                            parser_name,
                                            num_partitions,
                                            replication_factor,
                                            retention_bytes))
        Logger.info("Creating topics for error handling")
        Execute(command_template.format(self.__params.kafka_bin_dir,
                                        self.__params.zookeeper_quorum,
                                        "parser_invalid",
                                        num_partitions,
                                        replication_factor,
                                        retention_bytes))
        Execute(command_template.format(self.__params.kafka_bin_dir,
                                        self.__params.zookeeper_quorum,
                                        "parser_error",
                                        num_partitions, replication_factor,
                                        retention_bytes))
        Logger.info("Done creating Kafka topics")

    def init_parser_config(self):
        Logger.info('Loading parser config into ZooKeeper')
        Execute(format(
            "{metron_home}/bin/zk_load_configs.sh --mode PUSH -i {metron_zookeeper_config_path} -z {zookeeper_quorum}"),
            path=format("{java_home}/bin")
        )

    def start_parser_topologies(self):
        Logger.info("Starting Metron parser topologies: {}".format(self.get_parser_list()))
        start_cmd_template = """{}/bin/start_parser_topology.sh \
                                    -k {} \
                                    -z {} \
                                    -s {}"""
        for parser in self.get_parser_list():
            Logger.info('Starting ' + parser)
            Execute(start_cmd_template.format(self.__params.metron_home, self.__params.kafka_brokers,
                                              self.__params.zookeeper_quorum, parser))

        Logger.info('Finished starting parser topologies')

    def stop_parser_topologies(self):
        Logger.info('Stopping parsers')
        for parser in self.get_parser_list():
            Logger.info('Stopping ' + parser)
            stop_cmd = 'storm kill ' + parser
            Execute(stop_cmd)
        Logger.info('Done stopping parser topologies')

    def restart_parser_topologies(self):
        Logger.info('Restarting the parser topologies')
        self.stop_parser_topologies()
        attempt_count = 0
        while self.topologies_active():
            if attempt_count > 2:
                raise Exception("Unable to stop topologies")
            attempt_count += 1
            time.sleep(10)
        self.start_parser_topologies()
        Logger.info('Done restarting the parser topologies')

    def topologies_active(self):
        cmd_open = subprocess.Popen(["storm", "list"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = cmd_open.communicate()
        stdout_lines = stdout.splitlines()
        all_active = False
        if stdout_lines:
            all_active = True
            status_lines = self.__get_status_lines(stdout_lines)
            for parser in self.get_parser_list():
                parser_found=False
                is_active=False
                for line in status_lines:
                    items = re.sub('[\s]+', ' ', line).split()
                    if items and items[0] == parser:
                        status = items[1]
                        parser_found=True
                        is_active = self.__is_active(status)
                all_active &= parser_found and is_active
        return all_active

    def __get_status_lines(self, lines):
        status_lines=[]
        do_stat = False
        skipped = 0
        for line in lines:
            if line.startswith("Topology_name"):
                do_stat = True
            if do_stat and skipped == 2:
                status_lines += [line]
            elif do_stat:
                skipped += 1
        return status_lines

    def __is_active(self, status):
        return status == self.StormStatus.ACTIVE
