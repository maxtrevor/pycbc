# Copyright (C) 2020 Max Trevor and Derek Davis
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
import logging
from ligo import segments
from pycbc.workflow.core import FileList, Executable, Node, File
from pycbc.workflow.datafind import setup_datafind_workflow

class PyCBCCalculateDQExecutable(Executable):
    current_retention_level = Executable.ALL_TRIGGERS
    def create_node(self, segment, frames):
        start = int(segment[0])
        end = int(segment[1])
        node = Node(self)
        node.add_input_list_opt('--frame-files', frames)
        node.add_opt('--gps-start-time', start)
        node.add_opt('--gps-end-time', end)
        node.new_output_file_opt(segment, '.hdf', '--output-file')        
        return node

class PyCBCRerankDQExecutable(Executable):    
    current_retention_level = Executable.MERGED_TRIGGERS
    def create_node(self, workflow, ifo, dq_type, dq_files, binned_rate_file):
        node = Node(self)
        node.add_opt('--dq-type', dq_type)
        node.add_opt('--ifo', ifo)
        node.add_input_list_opt('--input-file', dq_files)
        node.add_input_opt('--rate-file', binned_rate_file)
        node.new_output_file_opt(workflow.analysis_time, '.hdf', '--output-file')        
        return node
    
class PyCBCBinTriggerRatesDQExecutable(Executable):
    current_retention_level = Executable.MERGED_TRIGGERS
    def create_node(self, workflow, ifo, dq_files, trig_file, bank_file):
        node = Node(self)
        node.add_opt('--ifo', ifo)
        node.add_input_opt('--bank-file', bank_file)
        node.add_input_opt('--trig-file', trig_file)
        node.add_input_list_opt('--dq-file', dq_files)
        node.new_output_file_opt(workflow.analysis_time,'.hdf', '--output-file')
        return node
    
def setup_dq_reranking(workflow, dq_label, insps, bank,
                        segs, analyzable_file, 
                        output_dir=None, tags=None):
    if tags:
        dq_tags = tags + [dq_label]
    else:
        dq_tags = [dq_label]
    datafind_files, dq_file, dq_segs, dq_name = \
                                       setup_datafind_workflow(workflow,
                                       segs, "datafind_dq",
                                       seg_file=analyzable_file,
                                       tags=dq_tags)
    output = FileList()
    for ifo in workflow.ifos:
        
        ifo_insp = [insp for insp in insps if (insp.ifo == ifo)]
        assert len(ifo_insp)==1
        ifo_insp = ifo_insp[0]
        
        dq_files = FileList()
        for seg in dq_segs[ifo]:
            seg_frames = datafind_files.find_all_output_in_range(ifo, seg)
            raw_exe  = PyCBCCalculateiDQExecutable(workflow.cp,
                                               'calculate_dq', ifos=ifo,
                                               out_dir=output_dir,
                                               tags=dq_tags)
            raw_node = raw_exe.create_node(seg, seg_frames)
            workflow += raw_node
            dq_files += raw_node.output_files
            
        intermediate_exe = PyCBCBinTriggerRatesiDQExecutable(workflow.cp,
                                               'bin_trigger_rates_dq', ifos=ifo,
                                               out_dir=output_dir,
                                               tags=dq_tags)
        intermediate_node = intermediate_exe.create_node(workflow, ifo, dq_files, 
                                                         ifo_insp, bank)
        workflow += intermediate_node
        binned_rate_file = intermediate_node.output_file
        
        new_exe = PyCBCRerankiDQExecutable(workflow.cp,
                                               'rerank_dq', ifos=ifo,
                                               out_dir=output_dir,
                                               tags=dq_tags)
        new_node = new_exe.create_node(workflow, ifo, dq_label, 
                                       dq_files, binned_rate_file)
        workflow += new_node
        output += new_node.output_files
    #else:
    #    msg = """No workflow-datafind section with dq tag.
    #          Tags must be used in workflow-datafind sections "
    #          if more than one source of data is used.
    #          Strain data source must be tagged 
    #          workflow-datafind-hoft.
    #          Consult the documentation for more info."""
    #    raise ValueError(msg)
               
    return output
