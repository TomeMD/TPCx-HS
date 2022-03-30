/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  @author Karthik Kulkarni 
 *
 */
package es.udc.tpcx_hs.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class HSDataCheck {

    public static void main(String args[]) {
            String hdfsPath = args[0];
		    Configuration conf = new Configuration();
		    //conf.addResource(new Path(FileUtilConstants.ENV_HADOOP_HOME + FileUtilConstants.REL_PATH_CORE_SITE));
		    //conf.addResource(new Path(FileUtilConstants.ENV_HADOOP_HOME + FileUtilConstants.REL_PATH_CORE_SITE));
		    conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020"+ args[0]);
		    try {
		        FileSystem fs = FileSystem.get(conf);
		        Path hdfsfilePath = new Path(hdfsPath);
		        //FileStatus status = fs.listStatus(hdfsfilePath);
		        if (!fs.exists(hdfsfilePath)) {
		            System.out.println("File does not exists : " + hdfsPath);
		        } else {
			        System.out.println("\nFile: " + hdfsPath);
			        if(fs.getFileStatus(hdfsfilePath).isDir()) {
					    FileStatus[] status = fs.listStatus(hdfsfilePath);
                        for(int i=0;i<status.length;i++) {
                            System.out.print(status[i].getPath() + "  - Length: ");
						    System.out.println(status[i].getLen() +" bytes");
                        }
                    } else {
		                System.out.println("File Size: " + fs.getFileStatus(hdfsfilePath).getLen());
				    }
		        }
		     } catch (Exception e) {
		            e.printStackTrace();
		     }
    }
}


 