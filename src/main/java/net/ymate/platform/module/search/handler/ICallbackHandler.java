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
package net.ymate.platform.module.search.handler;

import net.ymate.platform.module.search.ISearchable;

/**
 * @author 刘镇 (suninformation@163.com) on 14/12/16 上午9:20
 * @version 1.0
 */
public interface ICallbackHandler {

    /**
     * 当索引创建成功时调用此接口方法
     *
     * @param searchable
     */
    public void onIndexCreated(ISearchable searchable);

    /**
     * 当索引更新成功时调用此接口方法
     *
     * @param searchable
     */
    public void onIndexUpdated(ISearchable searchable);

    /**
     * 当索引删除成功时调用此接口方法
     *
     * @param searchableClass
     * @param id
     */
    public void onIndexRemoved(Class<? extends ISearchable> searchableClass, final String id);

}
