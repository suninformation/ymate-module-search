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
package net.ymate.platform.module.search.support;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import net.ymate.platform.module.search.ISearchConfig;

/**
 * <p>
 * DefaultSearchConfig
 * </p>
 * <p>
 * 
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
 *          <td>2014年3月9日上午2:07:50</td>
 *          </tr>
 *          </table>
 */
public class DefaultSearchConfig implements ISearchConfig {

	private String directoryPath;
	private Version luceneVersion;
	private Analyzer analyzerImpl;
	private int threadPoolSize;
	private double bufferSize;
	private int scheduledPeriod;

	/**
	 * 构造器
	 */
	public DefaultSearchConfig() {
	}

	/**
	 * 构造器
	 * 
	 * @param directoryPath
	 * @param luceneVersion
	 * @param analyzerImpl
	 * @param threadPoolSize
	 * @param bufferSize
	 * @param scheduledPeriod
	 */
	public DefaultSearchConfig(String directoryPath, Version luceneVersion,
			Analyzer analyzerImpl, int threadPoolSize, double bufferSize,
			int scheduledPeriod) {
		this.directoryPath = directoryPath;
		this.luceneVersion = luceneVersion;
		this.analyzerImpl = analyzerImpl;
		this.threadPoolSize = threadPoolSize;
		this.bufferSize = bufferSize;
		this.scheduledPeriod = scheduledPeriod;
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.search.ISearchConfig#getDirectoryPath()
	 */
	public String getDirectoryPath() {
		return directoryPath;
	}

	public void setDirectoryPath(String directoryPath) {
		this.directoryPath = directoryPath;
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.search.ISearchConfig#getLuceneVersion()
	 */
	public Version getLuceneVersion() {
		return luceneVersion == null ? Version.LUCENE_46 : luceneVersion;
	}

	public void setLuceneVersion(Version luceneVersion) {
		this.luceneVersion = luceneVersion;
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.search.ISearchConfig#getAnalyzerImpl()
	 */
	public Analyzer getAnalyzerImpl() {
		if (analyzerImpl == null) {
			return new StandardAnalyzer(getLuceneVersion());
		}
		return analyzerImpl;
	}

	public void setAnalyzerImpl(Analyzer analyzerImpl) {
		this.analyzerImpl = analyzerImpl;
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.search.ISearchConfig#getThreadPoolSize()
	 */
	public int getThreadPoolSize() {
		if (threadPoolSize <= 0) {
			return Runtime.getRuntime().availableProcessors() * 2 + 1;
		}
		return threadPoolSize;
	}

	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.search.ISearchConfig#getBufferSize()
	 */
	public double getBufferSize() {
		return bufferSize > 0 ? bufferSize : 16;
	}

	public void setBufferSize(double bufferSize) {
		this.bufferSize = bufferSize;
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.search.ISearchConfig#getScheduledPeriod()
	 */
	public int getScheduledPeriod() {
		return scheduledPeriod > 0 ? scheduledPeriod : 30;
	}

	public void setScheduledPeriod(int scheduledPeriod) {
		this.scheduledPeriod = scheduledPeriod;
	}

}
