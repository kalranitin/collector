/**
 * GlobalLBWideIPLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package iControl;

public class GlobalLBWideIPLocator extends org.apache.axis.client.Service implements iControl.GlobalLBWideIP {

/**
 * The WideIP interface enables you to work with wide IPs, as well
 * as with the pools and the virtual servers 
 *  that make them up.  For example, use the WideIP interface to get
 * a list of wide IPs, to add a wide IP, or 
 *  to remove a wide IP.
 */

    public GlobalLBWideIPLocator() {
    }


    public GlobalLBWideIPLocator(org.apache.axis.EngineConfiguration config) {
        super(config);
    }

    public GlobalLBWideIPLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
        super(wsdlLoc, sName);
    }

    // Use to get a proxy class for GlobalLBWideIPPort
    private java.lang.String GlobalLBWideIPPort_address = "https://url_to_service";

    public java.lang.String getGlobalLBWideIPPortAddress() {
        return GlobalLBWideIPPort_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String GlobalLBWideIPPortWSDDServiceName = "GlobalLB.WideIPPort";

    public java.lang.String getGlobalLBWideIPPortWSDDServiceName() {
        return GlobalLBWideIPPortWSDDServiceName;
    }

    public void setGlobalLBWideIPPortWSDDServiceName(java.lang.String name) {
        GlobalLBWideIPPortWSDDServiceName = name;
    }

    public iControl.GlobalLBWideIPPortType getGlobalLBWideIPPort() throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
            endpoint = new java.net.URL(GlobalLBWideIPPort_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getGlobalLBWideIPPort(endpoint);
    }

    public iControl.GlobalLBWideIPPortType getGlobalLBWideIPPort(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            iControl.GlobalLBWideIPBindingStub _stub = new iControl.GlobalLBWideIPBindingStub(portAddress, this);
            _stub.setPortName(getGlobalLBWideIPPortWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    public void setGlobalLBWideIPPortEndpointAddress(java.lang.String address) {
        GlobalLBWideIPPort_address = address;
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        try {
            if (iControl.GlobalLBWideIPPortType.class.isAssignableFrom(serviceEndpointInterface)) {
                iControl.GlobalLBWideIPBindingStub _stub = new iControl.GlobalLBWideIPBindingStub(new java.net.URL(GlobalLBWideIPPort_address), this);
                _stub.setPortName(getGlobalLBWideIPPortWSDDServiceName());
                return _stub;
            }
        }
        catch (java.lang.Throwable t) {
            throw new javax.xml.rpc.ServiceException(t);
        }
        throw new javax.xml.rpc.ServiceException("There is no stub implementation for the interface:  " + (serviceEndpointInterface == null ? "null" : serviceEndpointInterface.getName()));
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(javax.xml.namespace.QName portName, Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        if (portName == null) {
            return getPort(serviceEndpointInterface);
        }
        java.lang.String inputPortName = portName.getLocalPart();
        if ("GlobalLB.WideIPPort".equals(inputPortName)) {
            return getGlobalLBWideIPPort();
        }
        else  {
            java.rmi.Remote _stub = getPort(serviceEndpointInterface);
            ((org.apache.axis.client.Stub) _stub).setPortName(portName);
            return _stub;
        }
    }

    public javax.xml.namespace.QName getServiceName() {
        return new javax.xml.namespace.QName("urn:iControl", "GlobalLB.WideIP");
    }

    private java.util.HashSet ports = null;

    public java.util.Iterator getPorts() {
        if (ports == null) {
            ports = new java.util.HashSet();
            ports.add(new javax.xml.namespace.QName("urn:iControl", "GlobalLB.WideIPPort"));
        }
        return ports.iterator();
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        
if ("GlobalLBWideIPPort".equals(portName)) {
            setGlobalLBWideIPPortEndpointAddress(address);
        }
        else 
{ // Unknown Port Name
            throw new javax.xml.rpc.ServiceException(" Cannot set Endpoint Address for Unknown Port" + portName);
        }
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(javax.xml.namespace.QName portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        setEndpointAddress(portName.getLocalPart(), address);
    }

}