/**
 * LocalLBProfileIIOPLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package iControl;

public class LocalLBProfileIIOPLocator extends org.apache.axis.client.Service implements iControl.LocalLBProfileIIOP {

/**
 * The ProfileIIOP interface enables you to manipulate a local load
 * balancer's IIOP profile.
 */

    public LocalLBProfileIIOPLocator() {
    }


    public LocalLBProfileIIOPLocator(org.apache.axis.EngineConfiguration config) {
        super(config);
    }

    public LocalLBProfileIIOPLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
        super(wsdlLoc, sName);
    }

    // Use to get a proxy class for LocalLBProfileIIOPPort
    private java.lang.String LocalLBProfileIIOPPort_address = "https://url_to_service";

    public java.lang.String getLocalLBProfileIIOPPortAddress() {
        return LocalLBProfileIIOPPort_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String LocalLBProfileIIOPPortWSDDServiceName = "LocalLB.ProfileIIOPPort";

    public java.lang.String getLocalLBProfileIIOPPortWSDDServiceName() {
        return LocalLBProfileIIOPPortWSDDServiceName;
    }

    public void setLocalLBProfileIIOPPortWSDDServiceName(java.lang.String name) {
        LocalLBProfileIIOPPortWSDDServiceName = name;
    }

    public iControl.LocalLBProfileIIOPPortType getLocalLBProfileIIOPPort() throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
            endpoint = new java.net.URL(LocalLBProfileIIOPPort_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getLocalLBProfileIIOPPort(endpoint);
    }

    public iControl.LocalLBProfileIIOPPortType getLocalLBProfileIIOPPort(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            iControl.LocalLBProfileIIOPBindingStub _stub = new iControl.LocalLBProfileIIOPBindingStub(portAddress, this);
            _stub.setPortName(getLocalLBProfileIIOPPortWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    public void setLocalLBProfileIIOPPortEndpointAddress(java.lang.String address) {
        LocalLBProfileIIOPPort_address = address;
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        try {
            if (iControl.LocalLBProfileIIOPPortType.class.isAssignableFrom(serviceEndpointInterface)) {
                iControl.LocalLBProfileIIOPBindingStub _stub = new iControl.LocalLBProfileIIOPBindingStub(new java.net.URL(LocalLBProfileIIOPPort_address), this);
                _stub.setPortName(getLocalLBProfileIIOPPortWSDDServiceName());
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
        if ("LocalLB.ProfileIIOPPort".equals(inputPortName)) {
            return getLocalLBProfileIIOPPort();
        }
        else  {
            java.rmi.Remote _stub = getPort(serviceEndpointInterface);
            ((org.apache.axis.client.Stub) _stub).setPortName(portName);
            return _stub;
        }
    }

    public javax.xml.namespace.QName getServiceName() {
        return new javax.xml.namespace.QName("urn:iControl", "LocalLB.ProfileIIOP");
    }

    private java.util.HashSet ports = null;

    public java.util.Iterator getPorts() {
        if (ports == null) {
            ports = new java.util.HashSet();
            ports.add(new javax.xml.namespace.QName("urn:iControl", "LocalLB.ProfileIIOPPort"));
        }
        return ports.iterator();
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        
if ("LocalLBProfileIIOPPort".equals(portName)) {
            setLocalLBProfileIIOPPortEndpointAddress(address);
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