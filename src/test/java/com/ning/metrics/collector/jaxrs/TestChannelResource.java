/*
 * Copyright 2010-2014 Ning, Inc.
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
package com.ning.metrics.collector.jaxrs;

import com.ning.metrics.collector.processing.db.FeedEventStorage;
import com.ning.metrics.collector.processing.db.model.FeedEvent;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestChannelResource {

	private static final String CHANNEL = "channel";
	private static final int OFFSET = 0;
	private static final int LIMIT = 2;

	private ChannelResource search;
	
	private FeedEvent feedEvent;
	
	private FeedEventStorage feedEventStorage;
	
	@BeforeMethod(alwaysRun = true)
	private void setUp() {
		feedEventStorage = Mockito.mock(FeedEventStorage.class);
		search = new ChannelResource(feedEventStorage);
	}
	
	@Test
	public void testGetFeedEvent()
	{
		List<FeedEvent> feedEventsList = new ArrayList<FeedEvent>();
		feedEventsList.add(feedEvent);
		Mockito.when(feedEventStorage.loadFeedEventsByOffset(CHANNEL, OFFSET, LIMIT)).thenReturn(feedEventsList);
		
		Collection<FeedEvent> list = search.showChannels(CHANNEL, OFFSET, LIMIT);
		Assert.assertEquals(1, list.size());
	}
}
