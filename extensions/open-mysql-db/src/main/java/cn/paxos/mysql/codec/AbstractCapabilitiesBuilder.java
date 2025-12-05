/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql.codec;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

abstract class AbstractCapabilitiesBuilder<B extends AbstractCapabilitiesBuilder> {
	final Set<CapabilityFlags> capabilities = CapabilityFlags.getImplicitCapabilities();

	public B addCapabilities(CapabilityFlags... capabilities) {
		Collections.addAll(this.capabilities, capabilities);
		return (B) this;
	}

	public B addCapabilities(Collection<CapabilityFlags> capabilities) {
		this.capabilities.addAll(capabilities);
		return (B) this;
	}

	public boolean hasCapability(CapabilityFlags capability) {
		return capabilities.contains(capability);
	}

}
