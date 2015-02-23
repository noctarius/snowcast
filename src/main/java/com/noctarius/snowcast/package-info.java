/*
 * Copyright (c) 2014, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <h1>Introduction</h1>
 * <p>Generating unique IDs in a distributed system is a commonly known
 * hard problem to solve. It either is expensive, involves network traffic
 * or might generate conflicting IDs without recognizing it.</p>
 * <p>snowcast solves these and some other problems:
 * <ul>
 *     <li>guaranteed uniqueness</li>
 *     <li>low network interaction</li>
 *     <li>order guarantee (natural ordering)</li>
 *     <li>low latency, high rate generation</li>
 * </ul></p>
 * <p>snowcast uses 64bit wide data types to generate unique IDs. Where there
 * is no unsigned long in Java, the IDs might be negative but internally are
 * handled as unsigned anyways.</p>
 * <p>IDs are seen as a combination of 3 distinct sub-identifiers. That said
 * a snowcast sequence ID, schema-wise, looks like the following example:
 * <pre>
 *     | 41 bits                                | 13 bits    | 10 bits |
 * </pre></p>
 * <p>The first part of an ID defines the past milliseconds for a user defined
 * epoch. 41 bits offer about 69 years of milliseconds for any of these epochs.</p>
 * <p>The second part is defined the logical node ID. It is not always 13 bits
 * but ranges from 3 to 13 bits, depending on user preference. Looking at the
 * example, 13 bits (2^13) offers access to snowcast sequence IDs on 8,192 logical nodes.</p>
 * <p>The third and last part is a counter. This counter is unique per logical node
 * and millisecond. In this example it is 10 bits (2^10), offering 1,024 unique IDs
 * per millisecond on each of the logical nodes.</p>
 * <p>The second and third part can be increased or decreased based on user
 * preference. Please see
 * {@link com.noctarius.snowcast.Snowcast#createSequencer(java.lang.String, com.noctarius.snowcast.SnowcastEpoch, int)}.</p>
 * <h1>Usage of snowcast</h1>
 * <p>Since snowcast is built to work on top of Hazelcast, every snowcast instance
 * is created by passing in a {@link com.hazelcast.core.HazelcastInstance} to the
 * snowcast factory method.
 * <pre>
 *     HazelcastInstance hazelcastInstance = getHazelcastInstance();
 *     Snowcast snowcast = SnowcastSystem.snowcast( hazelcastInstance );
 * </pre></p>
 * <p>As learned above the next step is to define a custom epoch which provides
 * our base value for the milliseconds part of the snowcast sequence IDs. This is commonly
 * done using the Java Calendar API:
 * <pre>
 *     Calendar calendar = GregorianCalendar.getInstance();
 *     calendar.set( 2014, 1, 1, 0, 0, 0 );
 *     SnowcastEpoch epoch = SnowcastEpoch.byCalendar( calendar );
 * </pre>
 * Our epoch is now configured to have the point-zero on midnight of Jan 1st, 2014.</p>
 * <p>Preparations are done by now. Creating a {@link com.noctarius.snowcast.SnowcastSequencer}
 * using the {@link com.noctarius.snowcast.Snowcast} factory instance and the epoch,
 * together with a reference name, is now as easy as the following snippet:
 * <pre>
 *     SnowcastSequencer sequencer = snowcast.createSequencer( "sequencerName", epoch );
 * </pre>
 * That's it, that is the sequencer. It is immediately available to be used to create IDs.</p>
 * <p>Generating the IDs is now as easy as calling a method on the given sequencer instance:
 * <pre>
 *     long nextId = sequencer.next();
 * </pre></p>
 * <p>Eventually you either shutdown the Hazelcast cluster or you want to destroy a certain
 * {@link com.noctarius.snowcast.SnowcastSequencer} explicitly. This is done by passing the
 * sequencer instance to destroy to the {@link com.noctarius.snowcast.Snowcast} instance that
 * created it:
 * <pre>
 *     snowcast.destroySequencer( sequencer );
 * </pre>
 * Destroying a sequencer is a cluster operation and will destroy all sequencers referred to by
 * the same name on all nodes. After that point the existing SnowcastSequencer instances are in
 * a destroyed state and cannot be used anymore.</p>
 * <h1>Additional information</h1>
 * <p>For more information please find the github based documentation at
 * <a href="https://github.com/noctarius/snowcast">https://github.com/noctarius/snowcast</a>.</p>
 */
package com.noctarius.snowcast;
