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
package net.ymate.platform.module;

import java.util.Map;

import net.ymate.platform.base.AbstractModule;
import net.ymate.platform.commons.lang.BlurObject;
import net.ymate.platform.commons.util.ClassUtils;
import net.ymate.platform.module.search.Searchs;
import net.ymate.platform.module.search.support.DefaultSearchConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

/**
 * <p>
 * SearchModule
 * </p>
 * <p>
 * 全文检索框架模块加载器接口实现类；
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
 *          <td>2014年3月5日下午9:39:15</td>
 *          </tr>
 *          </table>
 */
public class SearchModule extends AbstractModule {

	/* (non-Javadoc)
	 * @see net.ymate.platform.module.base.IModule#initialize(java.util.Map)
	 */
	public void initialize(Map<String, String> moduleCfgs) throws Exception {
		DefaultSearchConfig _conf = new DefaultSearchConfig();
		_conf.setDirectoryPath(moduleCfgs.get("directory_path"));
		_conf.setLuceneVersion(Version.valueOf(StringUtils.defaultIfEmpty(moduleCfgs.get("lucene_version"), "LUCENE_46")));
		_conf.setThreadPoolSize(new BlurObject(moduleCfgs.get("thread_pool_size")).toIntValue());
		_conf.setBufferSize(new BlurObject(moduleCfgs.get("buffer_size")).toDoubleValue());
		_conf.setScheduledPeriod(new BlurObject(moduleCfgs.get("scheduled_period")).toIntValue());
		_conf.setAnalyzerImpl(ClassUtils.impl(moduleCfgs.get("analyzer_impl"), Analyzer.class, SearchModule.class));
		//
		Searchs.initialize(_conf);
	}

	/* (non-Javadoc)
	 * @see net.ymate.platform.module.base.IModule#destroy()
	 */
	public void destroy() throws Exception {
		Searchs.destroy();
	}

}
