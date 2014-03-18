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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.ymate.platform.base.YMP;
import net.ymate.platform.commons.i18n.I18N;
import net.ymate.platform.commons.util.ClassUtils;
import net.ymate.platform.module.search.ISearchable;
import net.ymate.platform.module.search.annotation.IndexField;
import net.ymate.platform.module.search.annotation.Indexed;

import org.apache.commons.lang.StringUtils;

/**
 * <p>
 * IndexedMeta
 * </p>
 * <p>
 * 索引模型元数据描述类；
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
 *          <td>2014年3月6日上午1:18:20</td>
 *          </tr>
 *          </table>
 */
public class IndexedMeta {

	public static final String FIELD_ID = "id";

	public static final String TARGET_CLASS_NAME = "__class";

	private String __indexName;

	/**
	 * 索引字段名称映射
	 */
	private Map<String, IndexField> __indexFieldMap = new HashMap<String, IndexField>();

	/**
	 * 构造器
	 * 
	 * @param searchableClass
	 */
	public IndexedMeta(Class<? extends ISearchable> searchableClass) {
		if (!ClassUtils.isInterfaceOf(searchableClass, ISearchable.class)) {
			throw new RuntimeException(I18N.formatMessage(YMP.__LSTRING_FILE, null, null, "ymp.search.searchable_class_need_impl", searchableClass.getName()));
		}
		Indexed _idx = searchableClass.getAnnotation(Indexed.class);
		if (null != _idx) {
			__indexName = StringUtils.defaultIfEmpty(_idx.name(), searchableClass.getSimpleName());
			this.__init(searchableClass);
		} else {
			throw new RuntimeException(I18N.formatMessage(YMP.__LSTRING_FILE, null, null, "ymp.search.searchable_class_need_anno_indexed", searchableClass.getName()));
		}
	}

	/**
	 * 初始化
	 * 
	 * @param searchableClass
	 */
	private void __init(Class<?> searchableClass) {
		// @IndexField
		for (Field _f : ClassUtils.getFields(searchableClass, true)) {
			IndexField _idxField = _f.getAnnotation(IndexField.class);
			if (_idxField != null) {
				__indexFieldMap.put(_f.getName(), _idxField);
			}
		}
	}

	public boolean isEmpty() {
		return __indexFieldMap.isEmpty();
	}

	/**
	 * @return 返回索引名称
	 */
	public String getIndexName() {
		return __indexName;
	}

	/**
	 * @return 返回索引模型类的字段名称集合
	 */
	public Set<String> getFieldNames() {
		return Collections.unmodifiableSet(__indexFieldMap.keySet());
	}

	/**
	 * @param fieldName 字段名称
	 * @return 返回指定字段的索引配置
	 */
	public IndexField getIndexField(String fieldName) {
		return __indexFieldMap.get(fieldName);
	}

}
