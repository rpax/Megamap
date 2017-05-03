/*
 *   Copyright 2005 John Watkinson
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.larvalabs.megamap.test;

import com.larvalabs.megamap.MegaMapManager;
import com.larvalabs.megamap.MegaMap;

/**
 *
 * Simple program to test MegaMap.
 *
 * Todo: More advanced and rigorous testing, especially with multiple threads.
 *
 * @author John Watkinson
 */
public class MegaTest {

    public static void main(String[] args) {
        try {
            MegaMapManager mmm = MegaMapManager.getMegaMapManager();
            mmm.setDiskStorePath(".");
            {
                MegaMap map = mmm.createMegaMap("map1!()*", true, true);
                map.put(new Integer(1), "Yup1!");
                map.put(new Integer(2), "Yup2!");
                map.put(new Integer(3), "Yup3!");
                map.put(new Integer(4), "Yup4!");
                String s = (String) map.get(new Integer(4));
                System.out.println(s);
                mmm.removeMegaMap("map1!()*");
            }
//            {
//                MegaMap map = mmm.createMegaMap("map2", "./tmp", false);
//                MegaMap map3= mmm.createMegaMap("map3", false);
//                map.put(new Integer(1), "Yup1!");
//                map.put(new Integer(2), "Yup2!");
//                map.put(new Integer(3), "Yup3!");
//                map.put(new Integer(4), "Yup4!");
//                map3.put(new Integer(10), "Mega10!");
//                map3.put(new Integer(11), "Mega11!");
//                String s = (String) map.get(new Integer(4));
//                String t = (String) map3.get(new Integer(11));
//                System.out.println(s + ", " + t);
//                mmm.removeMegaMap("map2");
//                String u = (String) map3.get(new Integer(10));
//                System.out.println(u);
//            }
            mmm.shutdown();
            mmm = MegaMapManager.getMegaMapManager();
            MegaMap map = mmm.createMegaMap("Map2", false, false);
            map.put("Blah", "OK");
            System.out.println("" + map.get("Blah"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
