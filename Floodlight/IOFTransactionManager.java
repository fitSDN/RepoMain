package net.floodlightcontroller.core.internal;

import java.util.List;

import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.types.DatapathId;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.mactracker.FlowRouteInfo;

public interface IOFTransactionManager extends IFloodlightService {

	long startTransaction(String callingFunction);
	
	void read(List<FlowRouteInfo[]> routeToInstall, long updateID, IOFSwitch sw, OFPacketIn pi);
	
	void transactionalUpdate(List<FlowRouteInfo[]> routeToInstall, long updateId, IOFSwitch sw, OFPacketIn pi);
}