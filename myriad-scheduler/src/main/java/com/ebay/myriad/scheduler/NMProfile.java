/**
 * Copyright 2015 PayPal
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ebay.myriad.scheduler;

import com.google.gson.Gson;

/**
 * Node Manager Profile
 */
public class NMProfile {
    private String name;

    /**
     * Number of CPU advertised to YARN Resource Manager.
     */
    private Double cpus;

    /**
     * Memory in MB advertised to YARN Resource Manager.
     */
    private Double memory;

    public NMProfile(String name, Double cpus, Double memory) {
        this.name = name;
        this.cpus = cpus;
        this.memory = memory;
    }

    public String getName() {
        return name;
    }

    public Double getCpus() {
        return cpus;
    }

    public Double getMemory() {
        return memory;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
