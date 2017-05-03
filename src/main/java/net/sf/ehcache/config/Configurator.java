/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2003 - 2004 Greg Luck.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution, if
 *    any, must include the following acknowlegement:
 *       "This product includes software developed by Greg Luck
 *       (http://sourceforge.net/users/gregluck) and contributors.
 *       See http://sourceforge.net/project/memberlist.php?group_id=93232
 *       for a list of contributors"
 *    Alternately, this acknowledgement may appear in the software itself,
 *    if and wherever such third-party acknowlegements normally appear.
 *
 * 4. The names "EHCache" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For written
 *    permission, please contact Greg Luck (gregluck at users.sourceforge.net).
 *
 * 5. Products derived from this software may not be called "EHCache"
 *    nor may "EHCache" appear in their names without prior written
 *    permission of Greg Luck.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL GREG LUCK OR OTHER
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by contributors
 * individuals on behalf of the EHCache project.  For more
 * information on EHCache, please see <http://ehcache.sourceforge.net/>.
 *
 */



package net.sf.ehcache.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.InputStream;
import java.net.URL;

/**
 * A utility class which configures beans from XML, using reflection.
 *
 * @version $Id: Configurator.java,v 1.1.1.1 2005/01/27 18:15:02 pents90 Exp $
 * @author Greg Luck
 */
public class Configurator {
    private static final Log LOG = LogFactory.getLog(Configurator.class.getName());

    private static final String DEFAULT_CLASSPATH_CONFIGURATION_FILE = "/ehcache.xml";
    private static final String FAILSAFE_CLASSPATH_CONFIGURATION_FILE = "/ehcache-failsafe.xml";

    /**
     * Constructor
     */
    public Configurator() {

    }

    /**
     * Configures a bean from an XML file.
     */
    public void configure(final Object bean, final File file) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring ehcache from file: " + file.toString());
        }
        final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        final BeanHandler handler = new BeanHandler(bean);
        parser.parse(file, handler);
    }

    /**
     * Configures a bean from an XML file available as an URL.
     */
    public void configure(final Object bean, final URL url) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring ehcache from URL: " + url);
        }
        final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        final BeanHandler handler = new BeanHandler(bean);
        parser.parse(url.toExternalForm(), handler);
    }

    /**
     * Configures a bean from an XML file in the classpath.
     */
    public void configure(final Object bean)
            throws Exception {
        final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        final BeanHandler handler = new BeanHandler(bean);
        URL url = getClass().getResource(DEFAULT_CLASSPATH_CONFIGURATION_FILE);
        if (url != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Configuring ehcache from ehcache.xml found in the classpath: " + url);
            }
        } else {
            url = getClass().getResource(FAILSAFE_CLASSPATH_CONFIGURATION_FILE);
            if (LOG.isWarnEnabled()) {
                LOG.warn("No configuration found. Configuring ehcache from ehcache-failsafe.xml"
                        + " found in the classpath: " + url);
            }
        }
        parser.parse(url.toExternalForm(), handler);
    }

    /**
     * Configures a bean from an XML input stream
     */
    public void configure(final Object bean, final InputStream inputStream) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring ehcache from InputStream");
        }
        final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        final BeanHandler handler = new BeanHandler(bean);
        parser.parse(inputStream, handler);
    }

}
