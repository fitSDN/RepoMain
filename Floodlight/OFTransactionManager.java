package net.floodlightcontroller.core.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.debugcounter.IDebugCounterService;
import net.floodlightcontroller.debugevent.IDebugEventService;
import net.floodlightcontroller.mactracker.AIMeasurements;
import net.floodlightcontroller.mactracker.AIMeasurements.AITimestampType;
import net.floodlightcontroller.mactracker.FM;
import net.floodlightcontroller.mactracker.FlowRouteInfo;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.topology.NodePortTuple;
import net.floodlightcontroller.util.FlowModUtils;

import org.projectfloodlight.openflow.protocol.OFBundleAddMsg;
import org.projectfloodlight.openflow.protocol.OFBundleCtrlMsg;
import org.projectfloodlight.openflow.protocol.OFBundleCtrlType;
import org.projectfloodlight.openflow.protocol.OFBundleFailedCode;
import org.projectfloodlight.openflow.protocol.OFErrorMsg;
import org.projectfloodlight.openflow.protocol.OFErrorType;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFFlowDelete;
import org.projectfloodlight.openflow.protocol.OFFlowDeleteStrict;
import org.projectfloodlight.openflow.protocol.OFFlowModCommand;
import org.projectfloodlight.openflow.protocol.OFFlowModify;
import org.projectfloodlight.openflow.protocol.OFFlowModifyStrict;
import org.projectfloodlight.openflow.protocol.OFFlowStatsEntry;
import org.projectfloodlight.openflow.protocol.OFFlowStatsReply;
import org.projectfloodlight.openflow.protocol.OFFlowStatsRequest;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFStatsReply;
import org.projectfloodlight.openflow.protocol.OFStatsType;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.errormsg.OFBundleFailedErrorMsg;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.BundleId;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFErrorCauseData;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OFTransactionManager implements IOFTransactionManager, IFloodlightModule, IOFMessageListener {
	protected static Logger logger = LoggerFactory.getLogger(OFTransactionManager.class);
	protected static OFFactory myFactory = OFFactories.getFactory(OFVersion.OF_14);
	
	protected static final int MMA_FLOW_ENTRY_PRIORITY = 11;
	protected static final int MMA_FLOW_ENTRY_LOW_PRIORITY = 9;
	protected static final int MMA_FLOW_ENTRY_IDLE_TIMEOUT_ACTIVE = 1;
	protected static final int MMA_FLOW_ENTRY_HARD_TIMEOUT_ACTIVE = 10000;
	protected static final int MMA_FLOW_ENTRY_IDLE_TIMEOUT_PROACTIVE = 10000;
	protected static final int MMA_FLOW_ENTRY_HARD_TIMEOUT_PROACTIVE = 10000;
	
	protected static final int NUMBER_OF_RETRY_ATTEMPTS = 0;
	
	protected static final int MIN_NUM_OF_THREADS = 4;
	protected static final int MAX_NUM_OF_THREADS = 10;
	protected static final int THREAD_QUEUE_SIZE = 500;
	
	public static final String MODULE_NAME = "TransactionManager";
	
	// Module Dependencies
	IFloodlightProviderService floodlightProvider;
	IDebugEventService debugEventService;
	IDebugCounterService debugCounterService;
	IOFSwitchService switchService;
	IStatisticsService statisticService;
	
	ExecutorService executor = null;
	ScheduledThreadPoolExecutor executorPool = null;
	
	protected static HashMap<Long, String> GlobalTransactionID = new HashMap<Long, String>();
	
	protected static HashMap<Long, List<FlowRouteInfo[]>> updateId2NetworkUpdateMap = new HashMap<Long, List<FlowRouteInfo[]>>();
	protected static HashMap<Long, HashSet<DatapathId>> updateId2SwList = new HashMap<Long, HashSet<DatapathId>>();
	protected static HashMap<Long, HashMap<DatapathId, Boolean>> updateId2SwResponses = new HashMap<Long, HashMap<DatapathId, Boolean>>();
	protected static HashMap<Long, HashMap<DatapathId, Long>> updateId2SwListMap2LastGtId = new HashMap<Long, HashMap<DatapathId, Long>>();
	protected static HashMap<Long, HashMap<DatapathId, SwitchReservationState> > updateId2SwitchStateMap = new HashMap<Long, HashMap<DatapathId, SwitchReservationState> >();
	
	protected static HashMap<Long, Integer> updateIdReattemptCounter = new HashMap<Long, Integer>();
	protected static List<Long> successfullyInstalled = new ArrayList<Long>();
	protected static List<Long> rollbackMap = new ArrayList<Long>();
	
	List<Long> proceed = new ArrayList<Long>();
	
	public String toString() {
		return MODULE_NAME;
	}
	
	public OFTransactionManager() {		
	}
	
	private class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {

	    @Override
	    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
	        System.out.println(r.toString() + " is rejected");
	    }

	}
	
	/* private void scheduleTask(Thread task, long delay) {
		executorPool.schedule(task, delay, TimeUnit.MILLISECONDS);
	} */
			
	private void vote(List<FlowRouteInfo[]> controlLogic, long updateId, IOFSwitch sw, OFPacketIn pi) {
		System.out.println("///////////////////// START VOTE updateID: " + updateId + ".............");
		
		for (int i = 0; i < controlLogic.size(); i++) {
			LinkedHashSet<DatapathId> switchList = new LinkedHashSet<DatapathId>(); 
			switchList.addAll(switchService.getAllSwitchDpids());
			
			addInstallationAttempt(updateId, switchList, controlLogic);
			updateIdReattemptCounter(updateId);
			
			AIMeasurements.getInstance().addAIMeasurements(AITimestampType.VOTE, System.currentTimeMillis(), 
					updateId, getSessionDescription(updateId), switchList.size());
			
			for (DatapathId swId : switchList) {
				String swID = swId.toString().substring(swId.toString().length() - 1);
				singleSwVote(swId, (updateId + Long.parseLong(swID)*1000000), controlLogic.get(i));
			}
		}
	}
	
	private void singleSwVote(DatapathId datapath, long updateID, FlowRouteInfo[] routeToInstall) {
		IOFSwitch sw = switchService.getGlobalSwitch(datapath);
		
		if (sw != null) {
			// Bundle Control
	        OFBundleCtrlMsg bundleControl = myFactory.buildBundleCtrlMsg()
	        		.setBundleId(BundleId.of((int)updateID))
	        		.setBundleCtrlType(OFBundleCtrlType.OPEN_REQUEST)
	        		.build();
	        sw.write(bundleControl);
	        
	        // Create transaction init primitive = FLOW_MOD with command MODIFY_STRICT and table_id 195 (0xc3 in the OVS)
	        ArrayList<OFAction> actions = null;
			Match match = null;
			
			Match.Builder mb = myFactory.buildMatch();
			mb.setExact(MatchField.ETH_TYPE, EthType.IPv4);
			match = mb.build();
			
			actions = new ArrayList<OFAction>();
			actions.add(myFactory.actions().output(OFPort.CONTROLLER, Integer.MAX_VALUE));
			
			List<OFInstruction> instructions = new ArrayList<OFInstruction>();
			instructions.add(myFactory.instructions().buildApplyActions().setActions(actions).build());
			
			OFFlowModifyStrict flow = myFactory.buildFlowModifyStrict()
					.setCookie(U64.ofRaw(11))
					.setXid(updateID)
					.setHardTimeout(getLastGtIdFirst(updateID, sw.getId()))
					.setIdleTimeout(getLastGtIdLast(updateID, sw.getId()))
					.setTableId(TableId.of(195))
					.setMatch(match)
					.setInstructions(instructions)
					.setBufferId(OFBufferId.NO_BUFFER)
					.build();
			
			// Add transaction init primitive to the bundle
			OFBundleAddMsg bundleAdd = myFactory.buildBundleAddMsg()
					.setXid(updateID)
	        		.setBundleId(BundleId.of((int)updateID))
	        		.setData(flow)
	        		.build();
			
			sw.write(bundleAdd);
			
			addNetworkUpdateToVote(updateID, sw.getId());
			
			// Bundle Commit = End of Vote
	        bundleControl = myFactory.buildBundleCtrlMsg()
	        		.setBundleId(BundleId.of((int)updateID))
	        		.setBundleCtrlType(OFBundleCtrlType.COMMIT_REQUEST)
	        		.build();
	        sw.write(bundleControl);
	        
	        System.out.println("(OFAtomicUpdateManager) - Vote >> sw: " + datapath + ", updateID: " + updateID);
		} else {
			throw new Error("Vote Failure ---- Datapath " + datapath.toString() + " does not exist!");
		}
	}
	
	private int getLastGtIdFirst(long updateID, DatapathId sw) {
		updateID %= (int) Math.pow(10, (int) Math.log10(updateID));
		return splitLastGtId(getLastGtId(updateID, sw),0);
	}
	
	private int getLastGtIdLast(long updateID, DatapathId sw) {
		updateID %= (int) Math.pow(10, (int) Math.log10(updateID));
		return splitLastGtId(getLastGtId(updateID, sw),4);
	}
	
	private int splitLastGtId(long gtId, int startIndex) {
		if (gtId > 0) {
			String hexString = Long.toHexString(gtId).toUpperCase();
			
			// pad with zeros!
			int length = hexString.length();
			for (int i = 0; i < (8 - length); i++) {
				hexString = "0" + hexString;
			}
			
			String substr = "";
			
			if (startIndex == 0) {
				substr = hexString.substring(startIndex, 4);
			} else {
				substr = hexString.substring(startIndex);
			}
			
			return Integer.parseInt(substr, 16);
		} else {
			return 0;
		}
	}

	private void commit(long updateId, boolean commit) {
		System.out.println("///////////////////// COMMIT updateID: " + updateId + ".............");
		Set<DatapathId> switchList = new HashSet<DatapathId>(); 
		switchList.addAll(getSwitchesForUpdateId(updateId));
		
		for (DatapathId sw : switchList) {
			String swID = sw.toString().substring(sw.toString().length() - 1);
			commitNetworkUpdateForSwitch((updateId + Long.parseLong(swID)*1000000), sw, commit);
		}
		
		// pushFirstPacketIn(updateId);
		
		if (commit) {
			AIMeasurements.getInstance().addAIMeasurements(AITimestampType.COMMIT, System.currentTimeMillis(), updateId, getSessionDescription(updateId), switchList.size());
		} else {
			AIMeasurements.getInstance().addAIMeasurements(AITimestampType.ROLLBACK, System.currentTimeMillis(), updateId, getSessionDescription(updateId), switchList.size());
		}
	}
	
	private String getSessionDescription(long updateId) {
		FlowRouteInfo[] networkUpdate = getNetworkUpdate(updateId).get(0);
		return networkUpdate[0].srcSW + "->" + networkUpdate[0].dstSW;
	}

	private void addNetworkUpdateToVote(long token, DatapathId swId) {
		FlowRouteInfo[] networkUpdate = getNetworkUpdate((token % (int) Math.pow(10, (int) Math.log10(token)))).get(0);
		
		if (networkUpdate == null) {
			System.out.println("(OFAtomicUpdateManager) - addNetworkUpdateToVote >> Network update is null, updateID: " + (token % (int) Math.pow(10, (int) Math.log10(token))));
			return;
		}
		
		for (int i = 0; i < networkUpdate.length; i++) {
			addFlowEntryToBundle(networkUpdate[i].route, networkUpdate[i].match, OFFlowModCommand.ADD, true, token, swId);
			break;
		}
	}
	
	private void addFlowEntryToBundle(Route route, Match match, OFFlowModCommand flowModCommand, boolean active, long updateID, DatapathId swId){
		OFPort outPort = null;
		
		List<NodePortTuple> npList = route.getPath();
		ArrayList<OFAction> actions = null;

		for (int i = npList.size()-2; i >= 0; i-=2) {
			if ((i == 0) || (i < (npList.size()-1))){
				DatapathId datapath = npList.get(i).getNodeId();
				if (!datapath.toString().equals(swId.toString())) {
					continue;
				}
				
				IOFSwitch sw = switchService.getGlobalSwitch(datapath);
				
				if ((i+1) < npList.size()) {
					outPort = npList.get(i+1).getPortId();
				} else {
					return;
				}
				
				if (match != null) {
					OFFlowAdd flow;

					actions = new ArrayList<OFAction>(); 
					actions.add(myFactory.actions().buildOutput()  
							.setPort(outPort)  
							.build());
					
					List<OFInstruction> instructions = new ArrayList<OFInstruction>();
					instructions.add(myFactory.instructions().buildApplyActions().setActions(actions).build());
										
					if (active) {
						flow = myFactory.buildFlowAdd()
								.setCookie(route.getId().getCookie())
								.setMatch(match)
								.setTableId(TableId.of(0))
								.setInstructions(instructions)
								.setBufferId(OFBufferId.NO_BUFFER)
								.setPriority(MMA_FLOW_ENTRY_PRIORITY)
								.setIdleTimeout(MMA_FLOW_ENTRY_IDLE_TIMEOUT_ACTIVE)
								.setHardTimeout(MMA_FLOW_ENTRY_HARD_TIMEOUT_ACTIVE)
								.build();
					} else {
						flow = myFactory.buildFlowAdd()
								.setCookie(route.getId().getCookie())
								.setMatch(match)
								.setTableId(TableId.of(0))
								.setInstructions(instructions) 
								.setBufferId(OFBufferId.NO_BUFFER)
								.setPriority(MMA_FLOW_ENTRY_PRIORITY)
								.setIdleTimeout(MMA_FLOW_ENTRY_IDLE_TIMEOUT_PROACTIVE)
								.setHardTimeout(MMA_FLOW_ENTRY_HARD_TIMEOUT_PROACTIVE)
								.build();
					}
					// need to build flow mod based on what type it is. Cannot set command later
					
					switch (flowModCommand) {
					case ADD:
						OFBundleAddMsg bundleAdd = myFactory.buildBundleAddMsg()
								.setXid(flow.getXid())
								.setBundleId(BundleId.of((int)updateID))
								.setData(flow)
								.build();
				
						sw.write(bundleAdd);
						break;
					case DELETE:
						OFFlowDelete flowDelete = FlowModUtils.toFlowDelete(flow);
						OFBundleAddMsg bundleDelete = myFactory.buildBundleAddMsg()
								.setXid(flowDelete.getXid())
								.setBundleId(BundleId.of((int)updateID))
				        		.setData(flowDelete)
				        		.build();
						
						sw.write(bundleDelete);
						break;
					case DELETE_STRICT:
						OFFlowDeleteStrict flowDelete_strict = FlowModUtils.toFlowDeleteStrict(flow);
						OFBundleAddMsg bundleDeleteStrict = myFactory.buildBundleAddMsg()
								.setXid(flowDelete_strict.getXid())
								.setBundleId(BundleId.of((int)updateID))
				        		.setData(flowDelete_strict)
				        		.build();
						
						sw.write(bundleDeleteStrict);
						break;
					case MODIFY:
						OFFlowModify flowModify = FlowModUtils.toFlowModify(flow);
						OFBundleAddMsg bundleModify = myFactory.buildBundleAddMsg()
								.setXid(flowModify.getXid())
				        		.setBundleId(BundleId.of((int)updateID))
				        		.setData(flowModify)
				        		.build();
						
						sw.write(bundleModify);
						break;
					default:
						System.out.println("(pushFlowEntry) Could not decode OFFlowModCommand)");        
						break;			
					}
				}
			}
		}		
	}
	
	private void commitNetworkUpdateForSwitch(long updateId, DatapathId datapath, boolean commit) {
		// Create COMMIT/REVERT primitive = FLOW_MOD with command DELETE/DELETE_STRICT and table_id 195 (0xc3 in the OVS)
		IOFSwitch sw = switchService.getGlobalSwitch(datapath);
        
		Match.Builder mb = myFactory.buildMatch();
		mb.setExact(MatchField.ETH_TYPE, EthType.IPv4);
		Match match = mb.build();
		
		ArrayList<OFAction> actions = new ArrayList<OFAction>();
		actions.add(myFactory.actions().output(OFPort.CONTROLLER, Integer.MAX_VALUE));
		
		List<OFInstruction> instructions = new ArrayList<OFInstruction>();
		instructions.add(myFactory.instructions().buildApplyActions().setActions(actions).build());
		
		OFFlowAdd flow = myFactory.buildFlowAdd()
				.setCookie(U64.ofRaw(11))
				.setTableId(TableId.of(195))
				.setMatch(match)
				.setInstructions(instructions)
				.setBufferId(OFBufferId.NO_BUFFER)
				.setXid(updateId)
				.build();
		
		if (commit) {
			OFFlowDelete flowDelete = FlowModUtils.toFlowDelete(flow);
			sw.write(flowDelete);
		} else {
			OFFlowDeleteStrict flowDeleteStrict = FlowModUtils.toFlowDeleteStrict(flow);
			sw.write(flowDeleteStrict);
		}
		
		System.out.println("(OFAtomicUpdateManager) - commitNetworkUpdateForSwitch >> SW: " + datapath.toString() + ", updateID: " + updateId);
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l =
				new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IOFTransactionManager.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>,
		IFloodlightService> m =
		new HashMap<Class<? extends IFloodlightService>,
		IFloodlightService>();
		// We are the class that implements the service
		m.put(IOFTransactionManager.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IDebugEventService.class);
		l.add(IDebugCounterService.class);
		l.add(IOFSwitchService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
		this.executorPool = new ScheduledThreadPoolExecutor(MAX_NUM_OF_THREADS, threadFactory, rejectionHandler);
		
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		debugEventService = context.getServiceImpl(IDebugEventService.class);
		debugCounterService = context.getServiceImpl(IDebugCounterService.class);
		switchService = context.getServiceImpl(IOFSwitchService.class);
		statisticService = context.getServiceImpl(IStatisticsService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider.addOFMessageListener(OFType.STATS_REPLY, this);
		floodlightProvider.addOFMessageListener(OFType.ERROR, this);
		floodlightProvider.addOFMessageListener(OFType.BUNDLE_CONTROL, this);
	}

	@Override
	public void transactionalUpdate(List<FlowRouteInfo[]> routeToInstall, long updateId, IOFSwitch sw, OFPacketIn pi) {
		vote(routeToInstall, updateId, sw, pi);
		/* Thread thread = new Thread(new vote(routeToInstall, updateId, sw, pi));
		thread.setPriority(Thread.MIN_PRIORITY);
		scheduleTask(thread, 0); */
	}
	
	/* public class vote implements Runnable {
		
		List<FlowRouteInfo[]> routeToInstall;
		IOFSwitch sw;
		OFPacketIn pi;
		long updateId;
		
		public vote(List<FlowRouteInfo[]> routeToInstall, long updateId, IOFSwitch sw, OFPacketIn pi) {
			this.routeToInstall = routeToInstall;
			this.sw = sw;
			this.pi = pi;
			this.updateId = updateId;
		}
		
		public void run() {
			vote(routeToInstall, updateId, sw, pi);
		}
	} */
	
	private void addInstallationAttempt(long updateId, HashSet<DatapathId> switchList, List<FlowRouteInfo[]> routeToInstall) {
		if (updateIdReattemptCounter.get(updateId) == null) {
			updateIdReattemptCounter.put(updateId, 0);
			updateId2NetworkUpdateMap.put(updateId, routeToInstall);
			updateId2SwitchStateMap.put(updateId, null);
		}
		
		HashMap<DatapathId, SwitchReservationState> switchReservationStateMap = new HashMap<DatapathId, SwitchReservationState>();
		for (DatapathId swId : switchList) {
			switchReservationStateMap.put(swId, SwitchReservationState.IDLE);
		}
		
		updateId2SwitchStateMap.put(updateId, switchReservationStateMap);
	}
	
	private void updateIdReattemptCounter(long updateId) {
		Integer counter = updateIdReattemptCounter.get(updateId);
		updateIdReattemptCounter.put(updateId, counter+1);
	}
	
	private Set<DatapathId> getSwitchesForUpdateId(long updateId) {
		return updateId2SwitchStateMap.get(updateId).keySet();
	}
	
	private boolean addRollback(long updateId) {
		if (!rollbackMap.contains(updateId)) {
			rollbackMap.add(updateId);
			return true;
		} else {
			return false;
		}
	}
	
	private void processReject(long updateId) {
		if (addRollback(updateId)) {
			if (getNetworkUpdate(updateId).get(0) != null && !isUpdateObsolate(updateId)) {
				setStateForUpdateId(updateId, SwitchReservationState.OBSOLETE);
				commit(updateId, false);
			}
		} else {
			// System.out.println("Racing conditions - rollback! Update ID: " + updateId);
		}
	}
	
	private void processConfirm(long updateId, DatapathId datapath) {
		//System.out.println("(OFAtomicUpdate) - processConfirm, swID: " + datapath.toString() + ", updateID: " + updateId);
		SwitchReservationState switchCurrentReservationState = getSwitchReservationState(datapath, updateId);
		if (switchCurrentReservationState == SwitchReservationState.OBSOLETE) {
			// System.out.println("Race conditions - prevention 1! Token: " + updateId);
			return;
		}
		
		if (switchCurrentReservationState == SwitchReservationState.BUNDLE_SUCCESS) {
			// System.out.println("Race conditions - prevention 2! Token: " + updateId);
			return;
		}
		
		if (updateId2SwitchStateMap.get(updateId) != null) {
			updateId2SwitchStateMap.get(updateId).put(datapath, SwitchReservationState.BUNDLE_SUCCESS);
			
			if (checkVoteOutcome(updateId)) {
				commit(updateId, true);
				
				FM fmInstance = new FM();
				fmInstance.fitSDNInstallationOutcome(updateId, true);
			}
		}
		
		return;
	}
	
	private SwitchReservationState getSwitchReservationState(DatapathId sw, long updateId) {
		if (updateId2SwitchStateMap.get(updateId) != null) {
			return updateId2SwitchStateMap.get(updateId).get(sw);
		}
		// System.out.println("ERROR! No token " + token + " recorded!");
		return null;
	}
	
	// This method is checking if all switches affected by the update (or only affected by the first leg of the update)
	// replied with confirm
	private boolean checkVoteOutcome(long updateId) {
		HashMap<DatapathId, SwitchReservationState> switchReservationStateMap = updateId2SwitchStateMap.get(updateId);
					
		for (DatapathId sw : getSwitchesForUpdateId(updateId)) {
			if(switchReservationStateMap.get(sw) != SwitchReservationState.BUNDLE_SUCCESS) {
				return false;
			}
		}
		return true;		
	}
	
	private boolean isUpdateObsolate(long updateId) {
		SwitchReservationState switchReservationState = null;
		HashMap<DatapathId,SwitchReservationState> switchReservationStateMap = updateId2SwitchStateMap.get(updateId);
		if (switchReservationStateMap != null) {
			Set<DatapathId> swSet = switchReservationStateMap.keySet();
			for(DatapathId swId:swSet) {
				switchReservationState = updateId2SwitchStateMap.get(updateId).get(swId);
				break;
			}
		}
		
		if (switchReservationState == SwitchReservationState.OBSOLETE) {
			return true;
		} else {
			return false;
		}
	}
	
	private void setStateForUpdateId(long updateId, SwitchReservationState state) {
		HashMap<DatapathId,SwitchReservationState> switchReservationStateMap = updateId2SwitchStateMap.get(updateId);
		
		if (switchReservationStateMap != null) {
			Set<DatapathId> swSet = switchReservationStateMap.keySet();
			
			for(DatapathId swId:swSet) {
				updateId2SwitchStateMap.get(updateId).put(swId, state);
			}
		}
	}
	
	private boolean checkUpdateIdReatemptCount(long updateId) {
		if (updateIdReattemptCounter.get(updateId) <= NUMBER_OF_RETRY_ATTEMPTS) {
			return true;
		}
		
		return false;
	}
	
	public long startTransaction(String callingFunction) {
		long ctrlID = switchService.getCtrlId()*100000;
		int max = 65535;
		int min = 0;
		
		Random rand = new Random();
		long updateID = rand.nextInt((max - min) + 1) + min;
			
		while (GlobalTransactionID.get(updateID) != null) {
			updateID = rand.nextInt((max - min) + 1) + min;
		}
 		
		GlobalTransactionID.put(updateID, callingFunction);
		updateID +=ctrlID;
		
		System.out.println("///////////////////// START TRANSACTION updateID: " + updateID + ".............");
		return updateID;
	}
	
	@Override
	public void read(List<FlowRouteInfo[]> routeToInstall, long updateId, IOFSwitch sw, OFPacketIn pi) {
		System.out.println("///////////////////// START READ updateID: " + updateId + ".............");
		updateId2NetworkUpdateMap.put(updateId, routeToInstall);
		
		HashSet<DatapathId> swList = new HashSet<DatapathId>();
		swList.addAll(switchService.getAllSwitchDpids());
		
		updateId2SwList.put(updateId, swList);
		initMap2LastGtId(updateId, swList);
		initId2SwListResponses(updateId);
		
		for (DatapathId datapath: swList) {
			String swID = datapath.toString().substring(datapath.toString().length() - 1);
			
			IOFSwitch mySwitch = switchService.getGlobalSwitch(datapath);
			
			OFFlowStatsRequest featuresRequest = mySwitch.getOFFactory().buildFlowStatsRequest()
					.setOutPort(OFPort.ANY)
					.setTableId(TableId.of(0))
					.setXid(updateId + Long.parseLong(swID)*1000000)
					.build();
			
			// System.out.println("Read start, sw: " + datapath.toString() + ", token: " + (updateID + Long.parseLong(swID)*1000000) + ", at time " + System.nanoTime());
			
			AIMeasurements.getInstance().addAIMeasurements(AITimestampType.READ_START, System.currentTimeMillis(), 
					updateId, datapath.toString(), -1);
			
			mySwitch.write(featuresRequest);
		}
	}

	private void recordReadResponse(long updateId, DatapathId sw, long lastGtId, boolean outcome) {
		setLastGtId(updateId, sw, lastGtId);
		
		updateId2SwResponses.get(updateId).put(sw, outcome);
		
		if (allReadResponsesArrived(updateId)) {
			// System.out.println("(OFReceiveReadData) | Received statistics for updateId: " + updateId);
			if (!proceed.contains(updateId)) {
				proceed.add(updateId);
				
				// Class.forName(className).getConstructor(String.class).newInstance();
				// Object classInstance = Class.forName(GlobalTransactionID.get(updateId)).getConstructor(String.class).newInstance();
				FM fmInstance = new FM();
				fmInstance.fitSDNInstallationReadFinished(updateId, 0);
				
				// transactionalUpdate(getNetworkUpdate(updateId), updateId, getSwitch(updateId), getPacketIn(updateId));
			}
		}
	}

	private void initMap2LastGtId(long updateID, HashSet<DatapathId> swList) {
		updateId2SwListMap2LastGtId.put(updateID, new HashMap<DatapathId, Long>());
		
		for (DatapathId sw : swList) {
			updateId2SwListMap2LastGtId.get(updateID).put(sw, (long) 0);
		}
	}
	
	private void setLastGtId(long updateID, DatapathId sw, long lastGtId) {
		updateId2SwListMap2LastGtId.get(updateID).put(sw, lastGtId);
	}
	
	private long getLastGtId(long updateID, DatapathId sw) {
		return updateId2SwListMap2LastGtId.get(updateID).get(sw);
	}
	
	private void initId2SwListResponses(long updateID) {
		updateId2SwResponses.put(updateID, new HashMap<DatapathId, Boolean>());
	}
	
	private List<FlowRouteInfo[]> getNetworkUpdate (long updateId) {
		return updateId2NetworkUpdateMap.get(updateId);
	}
	
	private boolean allReadResponsesArrived(long updateId) {
		if (updateId2SwResponses.get(updateId) == null || updateId2SwList.get(updateId) == null) {
			return false;
		}
		
		if (updateId2SwResponses.get(updateId).size() != updateId2SwList.get(updateId).size()) {
			return false;
		}
		return true;
	}

	@Override
	public String getName() {
		return MODULE_NAME;
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return (type.equals(OFType.STATS_REPLY) || type.equals(OFType.BUNDLE_CONTROL) || type.equals(OFType.ERROR));
	}

	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		if (forwardMsgToAnotherCtrl(msg)) {
			forwardToRemoteCtrl(msg);
			return Command.STOP;
		}
		
		switch (msg.getType()) {
			case STATS_REPLY:
				return this.processStatsMessage(sw, (OFStatsReply) msg, cntx);
			case ERROR:
				return this.processErrorMessage(sw, (OFErrorMsg) msg, cntx);
			case BUNDLE_CONTROL:
				return this.processBundleControlMessage(sw, (OFBundleCtrlMsg)msg, cntx);
			default:
				break;
		}
		return Command.CONTINUE;
	}
	
	private boolean forwardMsgToAnotherCtrl(OFMessage msg) {
		int ctrlId =  Integer.parseInt(getCtrlId(msg));
		System.out.println("(forwardMsgToAnotherCtrl) msg received ---> ctrlId: " + ctrlId);
		
		if (ctrlId == switchService.getCtrlId()) {
			return false;
		} else {
			return true;
		}
	}
	
	private void forwardToRemoteCtrl(OFMessage msg) {
		IOFSwitch remoteCtrl = switchService.getRemoteController(Integer.parseInt(getSwId(msg)), Integer.parseInt(getCtrlId(msg)));
		
		if (remoteCtrl == null) {
			System.out.println("TMRead | OFMessage cannot be fowarded! No connection with the remote ctrl!");
		}
		
		remoteCtrl.write(msg);
	}
	
	private String getCtrlId(OFMessage msg) {
		if (msg instanceof OFBundleCtrlMsg) {
			OFBundleCtrlMsg bundleMsg = (OFBundleCtrlMsg) msg;
			return String.valueOf(bundleMsg.getBundleId().getInt()).substring(1,2);
		} else {
			return String.valueOf(msg.getXid()).substring(1,2);
		}
	}
	
	private String getSwId(OFMessage msg) {
		if (msg instanceof OFBundleCtrlMsg) {
			OFBundleCtrlMsg bundleMsg = (OFBundleCtrlMsg) msg;
			return String.valueOf(bundleMsg.getBundleId().getInt()).substring(0,1);
		} else {
			return String.valueOf(msg.getXid()).substring(0,1);
		}
	}
	
	private DatapathId extractDatapathId(IOFSwitch sw, OFMessage msg) {
		DatapathId datapathId;
		if (sw.getId() == null) {
			datapathId = switchService.getRemoteSwitch(Integer.parseInt(getSwId(msg))).getId();
		} else {
			datapathId = switchService.getSwitch(sw.getId()).getId();
		}
		return datapathId;
	}
	
	private long readLastGtId(OFStatsReply statsReply) {
		OFFlowStatsReply fsr = (OFFlowStatsReply) statsReply;
		List<OFFlowStatsEntry> entries = fsr.getEntries();
		long lastGtId = 0;
		for (OFFlowStatsEntry entry : entries) {
			if (entry.getDurationNsec() > 0) {
				String hexString = Long.toHexString(entry.getDurationNsec()).toUpperCase();
				lastGtId = flipHexString(hexString);
			}
			break;
		}
		return lastGtId;
	}
	
	private long flipHexString(String hexString) {
		if (hexString.length()%2 == 1) {
			hexString = "0" + hexString;
		}
		
		StringBuilder  result = new StringBuilder();
		for (int i = hexString.length() - 2 ; i >= 0; i=i-2) {
			if (i == hexString.length() - 2) {
				result.append(new StringBuilder(hexString.substring(i)));
			} else {
				result.append(new StringBuilder(hexString.substring(i,i+2)));
			}
	    }
	    
	    return Long.parseLong(result.toString(), 16);
	}
	
	// Receive methods
	
	private Command processStatsMessage(IOFSwitch sw, OFStatsReply msg, FloodlightContext cntx) {
		OFStatsReply statsReply = (OFStatsReply)msg;
		long updateId = statsReply.getXid();
			
		// We need to remove the first digit in the updateID that represents the swId
		updateId = updateId % (int) Math.pow(10, (int) Math.log10(updateId));
			
		if (statsReply.getStatsType().equals(OFStatsType.FLOW)) {
			DatapathId datapathId = extractDatapathId(sw, msg);
			AIMeasurements.getInstance().addAIMeasurements(AITimestampType.READ_COMPLETE, System.currentTimeMillis(), 
					updateId, datapathId.toString(), -1);
				
			recordReadResponse(updateId, datapathId, readLastGtId(statsReply), true);
		} else {
			//System.out.println("TMRead | UNPROCESSED OFStatsType: " + statsReply.getStatsType());
		}
			
		return Command.STOP;
	}
	
	private Command processErrorMessage(IOFSwitch sw, OFErrorMsg msg, FloodlightContext cntx) {
		System.out.println("Error received!");
		if (msg.getErrType() == OFErrorType.BAD_ACTION) {
			// System.out.println("ERROR message received as a response to the attempt of configuration changes over a claimed switch");
			return Command.STOP;
		}
		
		// Bundle installation failed
		if (msg.getErrType() == OFErrorType.BUNDLE_FAILED) {
			OFBundleFailedErrorMsg bundleFailed = (OFBundleFailedErrorMsg) msg;
			
			OFBundleFailedCode code = bundleFailed.getCode();
			if (code == OFBundleFailedCode.TIMEOUT) {
				return Command.STOP;
			}
			
			OFErrorCauseData data = bundleFailed.getData();
			String errorString = data.toString();
			
			String left = "bundleId=";
			String right = ", bundleCtrlType";
			errorString = errorString.substring(errorString.indexOf(left) + left.length());
			errorString = errorString.substring(0, errorString.indexOf(right));
			long updateId = Long.parseLong(errorString);
			
			processReject(updateId);
		}
		
		return Command.CONTINUE;
	}
	
	private Command processBundleControlMessage(IOFSwitch sw, OFBundleCtrlMsg msg, FloodlightContext cntx) {
		if (msg.getBundleCtrlType() == OFBundleCtrlType.COMMIT_REPLY) {
			int updateId = msg.getBundleId().getInt();
			updateId = updateId % (int) Math.pow(10, (int) Math.log10(updateId));
			DatapathId datapath = extractDatapathId(sw, msg);
			processConfirm(updateId, datapath);
		}
		return Command.CONTINUE;
	}
}