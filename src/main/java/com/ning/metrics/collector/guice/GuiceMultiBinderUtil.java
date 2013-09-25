/*
 * Copyright 2010-2011 Ning, Inc.
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

package com.ning.metrics.collector.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.util.Types;

import java.util.Set;

public abstract class GuiceMultiBinderUtil extends AbstractModule
{
    <T> Key<Set<T>> bindMultibinderByClass(Iterable<? extends Class<? extends T>> contents, Class<T> superClass, Named annotation) {
        Multibinder<T> options = annotation == null ? Multibinder.newSetBinder(binder(), superClass) : Multibinder.newSetBinder(binder(), superClass, annotation);
        for (Class<? extends T> t : contents) {
           options.addBinding().to(t).asEagerSingleton();;
        }
        @SuppressWarnings("unchecked")
        final Key<Set<T>> multibinderKey = (Key<Set<T>>) Key.get(Types.setOf( superClass ));
        return multibinderKey;
     }
     
     <T> Key<Set<T>> bindMultibinderByLiteral(Iterable<? extends TypeLiteral<? extends T>> contents, TypeLiteral<T> superClass, Named annotation) {
           Multibinder<T> options = annotation == null ? Multibinder.newSetBinder(binder(), superClass) : Multibinder.newSetBinder(binder(), superClass, annotation);
           for (TypeLiteral<? extends T> t : contents) {
              options.addBinding().to(t);
           }
           @SuppressWarnings("unchecked")
           final Key<Set<T>> multibinderKey = (Key<Set<T>>) Key.get(Types.setOf(superClass.getType()));
           return multibinderKey;
        }
     
     <T> Key<Set<T>> bindMultibinderByKey(Iterable<? extends Key<? extends T>> contents, Key<T> superClass, Named annotation) {
         Multibinder<T> options = annotation == null ? Multibinder.newSetBinder(binder(), superClass.getTypeLiteral()) : Multibinder.newSetBinder(binder(), superClass.getTypeLiteral(), annotation);
         for (Key<? extends T> t : contents) {
           options.addBinding().to(t);
         }
         @SuppressWarnings("unchecked")
         final Key<Set<T>> multibinderKey =
             (Key<Set<T>>) Key.get(Types.setOf(superClass.getTypeLiteral().getType()));
         return multibinderKey;
       }
}
