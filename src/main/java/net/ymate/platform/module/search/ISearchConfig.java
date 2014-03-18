/*
 * Copyright 2007-2107 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.ymate.platform.module.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

/**
 * <p>
 * ISearchConfig
 * </p>
 * <p>
 * 全文检索框架初始化配置接口；
 * </p>
 * 
 * @author 刘镇(suninformation@163.com)
 * @version 0.0.0
 *          <table style="border:1px solid gray;">
 *          <tr>
 *          <th width="100px">版本号</th><th width="100px">动作</th><th
 *          width="100px">修改人</th><th width="100px">修改时间</th>
 *          </tr>
 *          <!-- 以 Table 方式书写修改历史 -->
 *          <tr>
 *          <td>0.0.0</td>
 *          <td>创建类</td>
 *          <td>刘镇</td>
 *          <td>2014年3月5日下午9:22:01</td>
 *          </tr>
 *          </table>
 */
public interface ISearchConfig {

	/**
	 * @return 返回索引文件存放路径，若未指定或为空则采用内存存储
	 */
	public String getDirectoryPath();

	/**
	 * @return 返回使用的Lucene版本, 当前默认：LUCENE_46
	 */
	public Version getLuceneVersion();

	/**
	 * @return 返回索引分析器对象，若未指定则采用系统默认：StandardAnalyzer
	 */
	public Analyzer getAnalyzerImpl();

	/**
	 * @return 返回索引线程池大小，默认值：CPU*2+1
	 */
	public int getThreadPoolSize();

	/**
	 * @return 返回IndexWriter内存缓冲区大小，单位：MB，默认值：16
	 */
	public double getBufferSize();

	/**
	 * @return 返回预订执行时间间隔，单位：秒，默认值：30秒
	 */
	public int getScheduledPeriod();

}
