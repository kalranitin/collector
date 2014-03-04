/*
 * Copyright 2010-2013 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ning.metrics.collector.processing.db.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.NamedArgumentFinder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class InClauseExpander implements NamedArgumentFinder
{
    private static final Joiner JOINER = Joiner.on(",");

    private final Map<String, String> args;

    private final String expansion;

    public InClauseExpander(Iterable<String> elements)
    {
        this("__InClauseExpander_", elements);
    }

    public InClauseExpander(String customPrefix, Iterable<String> elements)
    {
        List<String> prefixed = Lists.newArrayList();
        Map<String, String> args = Maps.newLinkedHashMap();
        int i = 0;
        for (String element : elements) {
            String name = customPrefix + i++;
            args.put(name, element);
            prefixed.add(":" + name);
        }
        this.args = args;
        this.expansion = JOINER.join(prefixed);
    }

    public String getExpansion()
    {
        return expansion;
    }

    @Override
    public Argument find(final String name)
    {
        if (args.containsKey(name)) {
            return new Argument()
            {
                @Override
                public void apply(final int position, final PreparedStatement statement, final StatementContext ctx) throws SQLException
                {
                    statement.setString(position, args.get(name));
                }
            };
        }
        else {
            return null;
        }
    }
}
