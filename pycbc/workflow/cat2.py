# Copyright (C) 2021 Simone Mozzon
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

#
# =============================================================================
#
#                                   Preamble
#
# =============================================================================
#

import os
import logging, copy
from ligo import segments
from pycbc.workflow.core import FileList, Executable, Node, File
from pycbc.workflow.segment import parse_cat_ini_opt

class PyCBCCalculateDqExecutable(Executable):
    current_retention_level = Executable.ALL_TRIGGERS
    def create_node(self, workflow, seg_file, cat2_file, flag):
        node = Node(self)
        # Executable objects are initialized with ifo information
        node.add_opt('--ifo', self.ifo_string)
        node.add_opt('--flag', flag)
        node.add_input_opt('--science-segments', seg_file)
        node.add_input_opt('--cat2-segments', cat2_file)
        node.new_output_file_opt(workflow.analysis_time, '.hdf', '--output-file')
        return node


def create_cat2_timeseries(workflow, seg_file, cat2_file, cat2_name, option_name,
                           output_dir=None, tags=None):
    if not workflow.cp.has_option('workflow-coincidence', 'do-dq-fitting'):
        return FileList()
    else:
        if not tags: tags=[]
        output = FileList()
        for ifo in workflow.ifos:
            flag_str = workflow.cp.get_opt_tags("workflow-segments", option_name, [ifo])
            flag_list = flag_str.split(',')
            for flag in flag_list:
                flag_name = flag[1:]
                logging.info("Creating job for flag %s" %(flag_name))
                flag_tag = copy.copy(tags)
                flag_tag.append(flag_name)
                raw_exe = PyCBCCalculateDqExecutable(workflow.cp,
                                                     'calculate_dq', ifos=ifo,
                                                     out_dir=output_dir,
                                                     tags=flag_tag)
                raw_node = raw_exe.create_node(workflow, seg_file, cat2_file, 
                                               flag_name)
                workflow += raw_node
                output += raw_node.output_files
    return output
