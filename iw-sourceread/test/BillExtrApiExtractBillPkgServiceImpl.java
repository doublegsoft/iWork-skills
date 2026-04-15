package com.hk.abpms.data.api.service.procedure.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hk.abpms.common.enums.YesOrNoEnum;
import com.hk.abpms.common.utils.common.DateUtils;
import com.hk.abpms.common.utils.common.StrUtils;
import com.hk.abpms.common.utils.common.VerifyUtils;
import com.hk.abpms.data.api.dao.billing.bill.*;
import com.hk.abpms.data.api.dao.billing.bitem.BitemMapper;
import com.hk.abpms.data.api.dao.billing.bitem.BitemReadMapper;
import com.hk.abpms.data.api.dao.collection.financialTransaction.FinTranMapper;
import com.hk.abpms.data.api.dao.customer.account.AccountMapper;
import com.hk.abpms.data.api.dao.customer.person.PersonMapper;
import com.hk.abpms.data.api.dao.customer.person.PersonNameMapper;
import com.hk.abpms.data.api.dao.customer.premise.PremiseMapper;
import com.hk.abpms.data.api.dao.customer.premise.PremiseZhtMapper;
import com.hk.abpms.data.api.dao.customer.svcDtl.SvcDtlContractValMapper;
import com.hk.abpms.data.api.dao.customer.svcDtl.SvcDtlMapper;
import com.hk.abpms.data.api.dao.procedure.CollApiReminderExtractionPkgMapper;
import com.hk.abpms.data.api.dao.setting.c.CfgCustTypeMapper;
import com.hk.abpms.data.api.dao.setting.s.CfgSvcTpAttrMapper;
import com.hk.abpms.data.api.service.procedure.BillExtrApiConterPkgService;
import com.hk.abpms.data.api.service.procedure.BillExtrApiExtractBillPkgService;
import com.hk.abpms.data.api.service.procedure.BillExtrApiGetPossibleSurchargePkgService;
import com.hk.abpms.data.api.utils.ProcedureUtil;
import com.hk.abpms.model.batch.extractBill.*;
import com.hk.abpms.model.dto.batch.bill.AccIdAndOpenItemSwDto;
import com.hk.abpms.model.dto.customer.account.FinTranDto;
import com.hk.abpms.model.dto.procedure.*;
import com.hk.abpms.model.entity.billing.bill.*;
import com.hk.abpms.model.entity.billing.bitem.BitemReadEntity;
import com.hk.abpms.model.entity.customer.account.CustAccountEntity;
import com.hk.abpms.model.entity.customer.person.PersonEntity;
import com.hk.abpms.model.entity.customer.person.PersonNameEntity;
import com.hk.abpms.model.entity.customer.premise.GeoAddressntity;
import com.hk.abpms.model.entity.customer.premise.PremiseZhtEntity;
import com.hk.abpms.model.entity.customer.svcDtl.SvcDtlContractValEntity;
import com.hk.abpms.model.entity.setting.c.CfgCustTypeEntity;
import com.hk.abpms.model.entity.setting.s.CfgSvcTpAttrEntity;
import com.hk.platform.common.bean.BusinessException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

/**
 * bill_extr_api_extract_bill_pkg包下的存储过程
 *
 * @author 郑海龙
 * @date 2025/3/5
 */
@Service
//@Slf4j
public class BillExtrApiExtractBillPkgServiceImpl implements BillExtrApiExtractBillPkgService {

    private static final Logger TRACER = LoggerFactory.getLogger(BillExtrApiExtractBillPkgServiceImpl.class);

    private final ThreadLocal<AlgorithmParameters> algorithmParams = ThreadLocal.withInitial(AlgorithmParameters::new);
    private final ThreadLocal<GlobalVariableBatch> blobalVariableBatch = ThreadLocal.withInitial(GlobalVariableBatch::new);
    private final ThreadLocal<GvGlobalVariableBatch> gvGlobalVariableBatch = ThreadLocal.withInitial(GvGlobalVariableBatch::new);
    private final ThreadLocal<GvBillExtractionHeaderBatch> gvBillExtractionHeaderBatch = ThreadLocal.withInitial(GvBillExtractionHeaderBatch::new);

    @Resource
    private BillExtrMapper billExtrMapper;
    @Resource
    private BillExtrApiConterPkgService billExtrApiConterPkgService;
    @Resource
    private BillExtrApiGetPossibleSurchargePkgService billExtrApiGetPossibleSurchargePkgService;

    @Resource
    private BillMapper billMapper;
    @Resource
    private BitemMapper bitemMapper;
    @Resource
    private AccountMapper accountMapper;
    @Resource
    private CfgCustTypeMapper cfgCustTypeMapper;
    @Resource
    private PersonNameMapper personNameMapper;
    @Resource
    private PersonMapper personMapper;
    @Resource
    private FinTranMapper finTranMapper;
    @Resource
    private SvcDtlMapper svcDtlMapper;
    @Resource
    private PremiseMapper premiseMapper;
    @Resource
    private PremiseZhtMapper premiseZhtMapper;
    @Resource
    private BillMsgMapper billMsgMapper;
    @Resource
    private BillExtrSvcDtlGttMapper billExtrSvcDtlGttMapper;
    @Resource
    private BillSummaryMapper billSummaryMapper;
    @Resource
    private BillRoutingMapper billRoutingMapper;
    @Resource
    private BillExtrDupTmpMapper billExtrDupTmpMapper;
    @Resource
    private BillMsgParamMapper billMsgParamMapper;
    @Resource
    private CollApiReminderExtractionPkgMapper  collApiReminderExtractionPkgMapper;
    @Resource
    private SvcDtlContractValMapper svcDtlContractValMapper;
    @Resource
    private BitemReadMapper bitemReadMapper;
    @Resource
    private CfgSvcTpAttrMapper cfgSvcTpAttrMapper;


    @Override
//    @Transactional(rollbackFor = Exception.class)
    public Map<String, String> extractBill(
            String piUserId,
            Date piProcessDttm,
            String piLanguageCd,
            String piBillId,
            int piSeqno,
            String piDebugMode,
            String piBatchMode,
            int piBillCnt,
            String piPrintedAndSent
    ) {
        // 清除历史记录
        billExtrMapper.delete(new QueryWrapper<BillExtrEntity>().lambda()
                .eq(BillExtrEntity::getBillId, piBillId));

        // 提前准备数据
        BillEntity bill = billMapper.selectById(piBillId);
        List<BillEntity> prevBills = billMapper.selectList(new QueryWrapper<BillEntity>().lambda()
            .lt(BillEntity::getCreatedDate, bill.getCreatedDate())
            .eq(BillEntity::getAccountId, bill.getAccountId())
            .orderByDesc(BillEntity::getCreatedDate));

        Set<String> limited30SvcIds = new HashSet<>();
        List<BitemWithReadAndQty> bitemReadList = billExtrMapper.selectBitemWithReadAndQty(piBillId);
        if (bitemReadList.size() >= 30) {
            for (BitemWithReadAndQty bitem : bitemReadList) {
                if (bitem.getSvcId() != null) {
                    limited30SvcIds.add(bitem.getSvcId());
                }
            }
        }
        List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList = collApiReminderExtractionPkgMapper.selectSvcDtlWithTypeAndBal(piBillId, new ArrayList<>());
        List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList = new ArrayList<>();
        if (!prevBills.isEmpty()) {
            prevSvcDtlWithTypeAndBalList = collApiReminderExtractionPkgMapper.selectSvcDtlWithTypeAndBal(prevBills.getFirst().getBillId(), new ArrayList<>());
        }

        List<FinTranWithAdj> finTranWithAdjList = collApiReminderExtractionPkgMapper.selectFinTranWithAdj(piBillId, bill.getBillDt());

        List<SvcDtlWithTypeAndBal> list1 = new ArrayList<>();
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            if (or("INSTALPN".equals(svc.getSvcTypeCode()),
                "DISPUTE".equals(svc.getSvcTypeCode()),
                svc.getGeoAddressId() == null)) {
                continue;
            }
            boolean foundInFt = false;
            for (FinTranWithAdj ft : finTranWithAdjList) {
                if (and(ft.getSvcId().equals(svc.getSvcId()),
                    "Y".equals(ft.getFreezeSw()),
                    "Y".equals(ft.getShowOnBillSw()),
                    or(in(ft.getFinTranTypeInd(), "BS", "BX"),
                        and(in(ft.getFinTranTypeInd(), "AD", "AX"), "XFER-DPF".equals(ft.getAdjTypeCode()))))) {
                    foundInFt = true;
                    break;
                }
            }
            if (foundInFt) {
                list1.add(svc);
            }
        }
        List<SvcDtlWithTypeAndBal> list2 = new ArrayList<>();
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            if (or(BigDecimal.ZERO.compareTo(svc.getCurAmt()) == 0,
                "OVERPAY".equals(svc.getSvcTypeCode()))) {
                continue;
            }
            boolean notFoundInList1 = true;
            for (SvcDtlWithTypeAndBal svcInList1 : list1) {
                if (svcInList1.getSvcId().equals(svc.getSvcId())) {
                    notFoundInList1 = false;
                    break;
                }
            }
            if (notFoundInList1) {
                list2.add(svc);
            }
        }

        List<String> bitemIds = new ArrayList<>();
        Set<String> rateIds = new HashSet<>();
        for (BitemWithReadAndQty bitem : bitemReadList) {
            bitemIds.add(bitem.getBitemId());
            if (bitem.getRateId() != null) {
                rateIds.add(bitem.getRateId().trim());
            }
        }
        Set<String> svcIds = new HashSet<>();
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            billExtrSvcDtlGttMapper.delete(new QueryWrapper<BillExtrSvcDtlGttEntity>()
                .lambda().eq(BillExtrSvcDtlGttEntity::getSvcId, svc.getSvcId()));
            svcIds.add(svc.getSvcId());
        }
        List<BitemCalcBdWithAttr> bitemCalcBdList = null;
        Set<String> contractValCodes = new HashSet<>();
        if (!bitemIds.isEmpty()) {
            bitemCalcBdList = billExtrMapper.selectBitemCalcBdWithAttr(bitemIds);
            for (BitemCalcBdWithAttr calcbd : bitemCalcBdList) {
                contractValCodes.add(calcbd.getAttrVal());
            }
        } else {
            bitemCalcBdList = new ArrayList<>();
        }
        List<RateWithDtlAndCrit> rateList = null;
        if (rateIds.isEmpty()) {
            rateList = new ArrayList<>();
        } else {
            rateList = billExtrMapper.selectRateWithDtlAndCrit(new ArrayList<>(rateIds));
        }
        List<SvcDtlContractValEntity> contractValEntityList = null;
        if (contractValCodes.isEmpty() || svcIds.isEmpty()) {
            contractValEntityList = new ArrayList<>();
        } else {
            contractValEntityList = svcDtlContractValMapper.selectList(new QueryWrapper<SvcDtlContractValEntity>().lambda()
                .in(SvcDtlContractValEntity::getContractValCode, contractValCodes)
                .in(SvcDtlContractValEntity::getSvcId, svcIds));
        }

        Map<String, String> outputParams = new HashMap<>();
        GvGlobalVariableBatch globalVariableBatch = gvGlobalVariableBatch.get();
        try {
            // 计算经过的秒数
            LocalDateTime now = LocalDateTime.now();
            int elapsedSeconds = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();

            // 设置调试模式和批处理模式
            globalVariableBatch.setGv_debug_mode((piDebugMode == null || piDebugMode.trim().isEmpty()) ? "N" : piDebugMode);
            globalVariableBatch.setGv_batch_mode(piBatchMode);
            String po_tmp_pid = "";
            int vCount1 = 0, vCount2 = 0, vCount3 = 0;
            String v_skip_bill_sw = "";
            String v_od_prm_amt = "";
            String v_od_prm_date = "";
            // 如果 pi_seqno > 1
            if (piSeqno > 1 /* 说明是重发副本 */) {
                /*!
                ** 此处的逻辑就是，要把账单提取数据，作为副本插入到提取数据副本表中。
                */
                vCount1 = billExtrMapper.countBillExtrByBillId(piBillId);
                // 如果 v_count1 > 0
                if (vCount1 > 0) {
                    // 查询 bill_routing 表
                    vCount2 = billExtrMapper.countBillRoutingByBillId(piBillId);
//                    System.out.println("bill_routing count: " + vCount2);

                    // 如果 v_count2 > 0
                    if (vCount2 > 0) {
                        // 查询 bill_attr 表
                        vCount3 = billExtrMapper.countBillAttrByBillId(piBillId);

                        // 如果 v_count3 > 0
                        if (vCount3 > 0) {
                            // 获取 SKIPBILL 的值
                            v_skip_bill_sw = billExtrMapper.getSkipBillSwByBillId(piBillId);

                            // 检查 SKIPBILL 的值
                            if ("N".equals(v_skip_bill_sw)) {
                                outputParams.put("po_skip_bill_sw", v_skip_bill_sw);
                                /*!
                                ** 生成账单提取副本，账单提取是用于为账单提供已经准备好的数据，
                                ** 而副本的目的则是为了优化程序效率，控制表的行数，副本的记录
                                ** 用了就删除。
                                */
                                BillExtrDuplicateBatchResponse billExtrResponse = billExtrDuplicate(piBillId, String.valueOf(piSeqno), piBillCnt);
                                double v_tot_amt_due = billExtrResponse.getPoAutopayAmt();
                                double v_autopay_amt = billExtrResponse.getPoTotAmtDue();
                                po_tmp_pid = billExtrResponse.getPoTmpPid();
                                if (StringUtil.isNotEmpty(po_tmp_pid) && po_tmp_pid.trim().length() >= 22) {
                                    //调用XQ034_CHECK_OVERDUE_MSG.ZS990_SETUP_SQL_S_OD_MSG存储过程
                                    Integer v_od_msg_cnt = zs990SetupSqlSOdMsg(piBillId, "CM_OVERDUE_MSG");
                                    if (v_od_msg_cnt > 0) {
                                        //调用XQ036_GET_OVERDUE_PARM.ZZ992_SETUP_SQL_S_BILL_PRM存储过程
                                        //BILL_MSG_PARAM表中SEQ = 1，2的 MSG_PARAM_VAL的信息BILL_MSG_PARAM表删除，pi_bill_msg_cd = 'OD33'
                                        BillMsgParamEntity od331 = billMsgParamMapper.selectOne(Wrappers.<BillMsgParamEntity>query().lambda()
                                                .eq(BillMsgParamEntity::getBillId, piBillId)
                                                .eq(BillMsgParamEntity::getSeq, 1)
                                                .eq(BillMsgParamEntity::getBillMsgCode, "OD33"));
                                        //调用XQ036_GET_OVERDUE_PARM.ZZ992_SETUP_SQL_S_BILL_PRM存储过程
                                        BillMsgParamEntity od332 = billMsgParamMapper.selectOne(Wrappers.<BillMsgParamEntity>query().lambda()
                                                .eq(BillMsgParamEntity::getBillId, piBillId)
                                                .eq(BillMsgParamEntity::getSeq, 2)
                                                .eq(BillMsgParamEntity::getBillMsgCode, "OD33"));
                                        v_od_prm_amt = od331.getMsgParamVal();
                                        v_od_prm_date = od332.getMsgParamVal();
                                    }
                                    //插入bill_extr表数据
                                    List<BillExtrDupTmpEntity> billExtrDupTmpEntities = billExtrDupTmpMapper.selectList(Wrappers.<BillExtrDupTmpEntity>query().lambda()
                                            .eq(BillExtrDupTmpEntity::getTmpTblPid, po_tmp_pid)
                                            .eq(BillExtrDupTmpEntity::getBillId, piBillId));
                                    // 定义日期格式
                                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
                                    // 格式化日期
                                    String formattedDate = sdf.format(piProcessDttm);
                                    // 拼接字符串
                                    String finalResult = formattedDate + ".000000";
//                                    for (BillExtrDupTmpEntity billExtrDupTmpEntity : billExtrDupTmpEntities) {
//                                        BillExtrEntity billExtrEntity = new BillExtrEntity();
//                                        billExtrEntity.setBillId(billExtrDupTmpEntity.getBillId());
//                                        billExtrEntity.setExtrLine(billExtrDupTmpEntity.getExtrLine());
//                                        billExtrEntity.setUserid(piUserId);
//                                        billExtrEntity.setProcDate(finalResult);
//                                        try {
//                                            billExtrMapper.insert(billExtrEntity);
//                                        } catch (Throwable cause) {
//
//                                        }
//                                    }
//                                    billExtrDupTmpMapper.delete(Wrappers.<BillExtrDupTmpEntity>query().lambda()
//                                            .eq(BillExtrDupTmpEntity::getTmpTblPid, po_tmp_pid)
//                                            .eq(BillExtrDupTmpEntity::getBillId, piBillId));
                                    BillRoutingEntity billRoutingEntity = billRoutingMapper.selectOne(Wrappers.<BillRoutingEntity>query().lambda()
                                            .eq(BillRoutingEntity::getBillId, piBillId)
                                            .eq(BillRoutingEntity::getSeq, piSeqno));
                                    String v_DO_NOT_EXTR_SW = billRoutingEntity.getDoNotExtrSw();
                                    String v_BILL_ROUTE_TYPE_CODE = billRoutingEntity.getBillRouteTypeCode();
                                    Date v_bill_routing_EXTR_DATE = billRoutingEntity.getExtrDate();
                                    if ("N".equals(v_skip_bill_sw) && "N".equals(v_DO_NOT_EXTR_SW)) {
                                        //调用XQ037_CHK_BILL_SUMMARY.ZZ994_SETUP_SQL_S_BILL_SUMM存储过程
                                        Integer v_bill_summary_cnt = billSummaryMapper.selectCount(Wrappers.<BillSummaryEntity>lambdaQuery().eq(BillSummaryEntity::getBillId,
                                                piBillId));
                                        BillEntity billEntity = billMapper.selectById(piBillId);
                                        String v_account_id = billEntity.getAccountId();
                                        Date v_DUE_DATE = billEntity.getDueDt();
                                        Date v_BILL_DATE = billEntity.getBillDt();
                                        if (v_bill_summary_cnt == 0) {
                                            //调用XQ038_INS_BILL_SUMMARY.ZZ993_SETUP_SQL_I_BILL_SUMM存储过程插入BILL_SUMMARY表数据
                                            BillSummaryEntity billSummaryEntity = new BillSummaryEntity();
                                            billSummaryEntity.setAccountId(v_account_id);
                                            billSummaryEntity.setBillId(piBillId);
                                            billSummaryEntity.setDueDate(v_DUE_DATE);
                                            billSummaryEntity.setBillIssuedDate(v_BILL_DATE);
                                            billSummaryEntity.setTotAmtDue(BigDecimal.valueOf(v_tot_amt_due));
                                            billSummaryEntity.setAutoPayAmt(BigDecimal.valueOf(v_autopay_amt));
                                            billSummaryEntity.setOvd3P1(v_od_prm_amt);
                                            billSummaryEntity.setOvd3P2(v_od_prm_date);
                                            billSummaryEntity.setBillExtSrc(v_BILL_ROUTE_TYPE_CODE);
                                            billSummaryMapper.insert(billSummaryEntity);
                                        }
                                    }
//                                    if ("ONLINE".equals(v_BILL_ROUTE_TYPE_CODE) && v_bill_routing_EXTR_DATE == null && piSeqno != 1 &&
//                                        !"N".equals(piDebugMode)) {
//                                        //修改bill_routing表数据
//                                        billRoutingMapper.updateDate(piBillId, piSeqno);
//                                    }
                                    if ("N".equals(piDebugMode)) {
                                        billRoutingMapper.updateDate(piBillId, piSeqno);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (StringUtil.isEmpty(po_tmp_pid)) {
                // 调用 AA000_MAIN 方法（假设已实现）
                Aa000MainDto aa000MainDto = new Aa000MainDto();
                aa000MainDto.setPiUserId(piUserId);
                aa000MainDto.setPiProcessDttm(piProcessDttm);
                aa000MainDto.setPiLanguageCd(piLanguageCd);
                aa000MainDto.setPiBillId(piBillId);
                aa000MainDto.setPiSeqno(piSeqno);
                aa000MainDto.setPiCopyCnt(1);
                aa000MainDto.setPiDebugMode(piDebugMode);
                aa000MainDto.setPiBatchMode(piBatchMode);
                aa000MainDto.setPiBillCnt(piBillCnt);
                aa000MainDto.setPiExtractFileName(" ");
                aa000MainDto.setPiAlgParmVal1("XFER-DPF");
                aa000MainDto.setPiAlgParmVal2("DSDCHG");
                aa000MainDto.setPiAlgParmVal3("BILL_INSTPL_SD");
                aa000MainDto.setPiAlgParmVal4("SEWAGE_SA_TYPE");
                aa000MainDto.setPiAlgParmVal5("TES_SA_TYPE");
                aa000MainDto.setPiAlgParmVal6("BILL_DISPUTE_SD");
                aa000MainDto.setPiAlgParmVal7("SCRT-BF");
                aa000MainDto.setPiAlgParmVal8("SCDF-BF");
                aa000MainDto.setPiAlgParmVal9("TESDF-BF");
                aa000MainDto.setPiAlgParmVal10("TESRT-BF");
                aa000MainDto.setPiAlgParmVal11("CM");
                aa000MainDto.setPiAlgParmVal12("GAL");
                aa000MainDto.setPiAlgParmVal13("IGNORE");
                aa000MainDto.setPiAlgParmVal14("IGNORENE");
                aa000MainDto.setPiAlgParmVal15("10");
                aa000MainDto.setPiAlgParmVal16("SURG");
                aa000MainDto.setPiAlgParmVal17("CM_DSD_CHARGE_BMSG");
                aa000MainDto.setPiAlgParmVal18("CRCV");
                aa000MainDto.setPiAlgParmVal19("STATCHG");
                aa000MainDto.setPiAlgParmVal20("WSDREP");
                aa000MainDto.setPiAlgParmVal21("CRB3");
                aa000MainDto.setPiAlgParmVal22("PCWTRCON");
                aa000MainDto.setPiAlgParmVal23("CRB1");
                aa000MainDto.setPiAlgParmVal24("CANCEL_REBILL_DESCR");
                aa000MainDto.setPiAlgParmVal25("FNB1");
                aa000MainDto.setPiAlgParmVal26("OVERPAY");
                aa000MainDto.setPiAlgParmVal27("BILL_CAN_RSN_EXCL");
                aa000MainDto.setPiAlgParmVal28("立方米");
                aa000MainDto.setPiAlgParmVal29("Y");
                aa000MainDto.setPiPrintedAndSent(piPrintedAndSent);
                String po_skip_bill_sw = aa000Main(aa000MainDto,
                    svcDtlWithTypeAndBalList, prevSvcDtlWithTypeAndBalList,
                    bitemReadList, bitemCalcBdList, finTranWithAdjList,
                    rateList, contractValEntityList);
                outputParams.put("po_skip_bill_sw", po_skip_bill_sw);

                if (globalVariableBatch.getGv_output_code() != null &&
                        globalVariableBatch.getGv_output_code().startsWith("E")) {
//                    System.out.println("end of bill_extr_api_extract_bill_pkg.AA000_MAIN but error: " + outputParams.get("po_output_code"));
                    return outputParams;
                }
                String vPdfTypeCode = "O";
                // 如果批处理模式为 'N' 且输出代码不以 'E' 开头且账单已打印并发送
                if ("N".equals(piBatchMode) &&
                    globalVariableBatch.getGv_output_code() != null &&
                    !globalVariableBatch.getGv_output_code().startsWith("E") && "Y".equals(piPrintedAndSent)) {

                    // 定义日期格式
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
                    // 格式化日期
                    String formattedDate = sdf.format(piProcessDttm);
                    // 拼接字符串
                    String pi_process_dttm = formattedDate + ".000000";
                    try {
                        int vPdfBillWorkInsertCnt = billExtrMapper.insertPdfBillWork(
                                vPdfTypeCode,
                                piBillId,
                                piSeqno,
                                piProcessDttm,
                                piUserId,
                                pi_process_dttm,
                                piLanguageCd
                        );
                        //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
                        zz000SetupSqlICiMsg(piBillId, "10", "extract_bill:insert PDFBILL_WORK count: " + vPdfBillWorkInsertCnt);

                        if (vPdfBillWorkInsertCnt == 0) {
                            globalVariableBatch.setGv_output_code("W999997");
                            globalVariableBatch.setGv_output_msg("Will not create PDF file via Bill Attr link");
                            return outputParams;
                        }

                        int vCnt = billExtrMapper.countPdfBillBatchCtrl(vPdfTypeCode, piProcessDttm);
                        if (vCnt == 0) {
                            billExtrMapper.insertPdfBillBatchCtrl(vPdfTypeCode, piProcessDttm);
                        } else if (vCnt == 1) {
                            billExtrMapper.updatePdfBillBatchCtrl(vPdfTypeCode, piProcessDttm);
                        }
                    } catch (Exception e) {
                        globalVariableBatch.setGv_output_code("W999997");
                        globalVariableBatch.setGv_output_msg("ERROR create PDF file via Bill Attr link: " + e.getMessage());
                        return outputParams;
                    }
                } else {
//                    System.out.println("pi_batch_mode: " + piBatchMode + ", output_code: " + outputParams.get("po_output_code") + ", pi_printed_and_sent: " + piPrintedAndSent);
                }

                if ("Y".equals(piPrintedAndSent) &&
                    globalVariableBatch.getGv_output_code() != null &&
                    globalVariableBatch.getGv_output_code().startsWith("W")) {
                    globalVariableBatch.setGv_output_code("W999996");
                    globalVariableBatch.setGv_output_msg("will not create PDF via Bill Attr link");
                }

//                System.out.println("The end of extract_bill.");
                if (globalVariableBatch.getGv_debug_msg() != null && globalVariableBatch.getGv_debug_msg().length() >= 2) {
                    // 截取前两个字符
                    String prefix = globalVariableBatch.getGv_debug_msg().substring(0, 2);
                    if ("_;".equals(prefix)) {
                        // 截取从第3个字符开始的字符串
                        globalVariableBatch.setGv_debug_msg(globalVariableBatch.getGv_debug_msg().substring(2));
                    }
                }
                outputParams.put("po_debug_msg", globalVariableBatch.getGv_debug_mode());
                outputParams.put("po_output_code", globalVariableBatch.getGv_output_code());
                outputParams.put("po_output_msg", globalVariableBatch.getGv_output_msg());
            }
            // 更新bill_routing的extr_date
            if ("N".equals(piDebugMode)) {
                billRoutingMapper.updateDate(piBillId, piSeqno);
            }
        } catch (Exception e) {
            // 异常处理
            TRACER.error(e.getMessage(), e);
            outputParams.put("po_output_code", "E999998");
            outputParams.put("po_output_msg", e.getMessage());
//            System.out.println("Exception occurred: " + e.getMessage());
            throw new RuntimeException("Exception occurred: " + e.getMessage());
        }
        return outputParams;
    }

    @Override
    public BillExtrDuplicateBatchResponse billExtrDuplicate(String pBillId, String pSeqno, int pBillCnt) {
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        BillExtrDuplicateBatchResponse response = new BillExtrDuplicateBatchResponse();
        String v_rc_name1 = null;
        String v_rc_name2 = null;
        String v_rc_name3 = null;
        String v_address1 = null;
        String v_address2 = null;
        String v_address3 = null;
        String v_address4 = null;
        String v_PERSON_ID = null;
        String v_BILL_ROUTE_TYPE_CODE = null;
        String v_return_val = "";
        String v_tmp_pid = null;
        String v_process_dttm = null;
        int v_bbpxt_payslp_serial_no = 0;
        try {
            if (pSeqno.length() > 2 || Integer.parseInt(pSeqno) <= 1) {
                response.setPoTmpPid("");
                return response;
            }
            /* Generate temp table PID*/
            // 模拟获取当前时间
            Date now = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
            String processDttm = sdf.format(now) + ".000000";

            // 生成随机数并格式化
            Random random = new Random();
            int randomNumber = random.nextInt(99999998) + 1;
            String rmNo = String.format("%08d", randomNumber);

            // 去除日期字符串中的特殊字符
            String formattedProcessDttm = processDttm.replace("-", "").replace(".000000", "").replace(".", "");

            // 拼接临时 ID
            v_tmp_pid = formattedProcessDttm + rmNo;

            // 模拟 INTO 操作，将结果存储在变量中
            v_process_dttm = processDttm;
        } catch (Exception e) {
            // 异常处理
            v_return_val = "E";
        }
        if (StringUtil.isEmpty(v_return_val)) {
            try {
                /* Find bill routing record*/
                BillRoutingEntity billRoutingEntity = billRoutingMapper.selectOne(Wrappers.<BillRoutingEntity>query().lambda()
                        .eq(BillRoutingEntity::getBillId, pBillId)
                        .eq(BillRoutingEntity::getSeq, pSeqno));
                v_rc_name1 = billRoutingEntity.getNameLine1();
                v_rc_name2 = billRoutingEntity.getNameLine2();
                v_rc_name3 = billRoutingEntity.getNameLine3();
                v_address1 = billRoutingEntity.getAddrLine1();
                v_address2 = billRoutingEntity.getAddrLine2();
                v_address3 = billRoutingEntity.getAddrLine3();
                v_address4 = billRoutingEntity.getAddrLine4();
                v_PERSON_ID = billRoutingEntity.getPersonId();
                v_BILL_ROUTE_TYPE_CODE = billRoutingEntity.getBillRouteTypeCode();
            } catch (Exception e) {
                // 异常处理
                v_return_val = "E";
            }
        }
        if (StringUtil.isEmpty(v_return_val)) {
            /* create duplicate bill record*/
            int i = billExtrDupTmpMapper.insertBillExtrDupTmp(v_tmp_pid, v_process_dttm, pBillId);
            if (i == 0) {
                v_return_val = "E";
            }
        }
        if (StringUtil.isEmpty(v_return_val)) {
            //replace SUBSTR(extr_line,30,2) sequence_number
            billExtrDupTmpMapper.updateBillExtrDupTmp(pSeqno, v_tmp_pid, pBillId);
        }
        if (StringUtil.isEmpty(v_return_val)) {
            if ("Y".equals(gvGlobalVariableBatch1.getGv_batch_mode())) {
                v_bbpxt_payslp_serial_no = pBillCnt + 1;
            } else {
                v_bbpxt_payslp_serial_no = 1;
            }
            billExtrDupTmpMapper.updateBillExtrDupTmp1(v_bbpxt_payslp_serial_no, v_tmp_pid, pBillId);
            //siesta20210826 '02' final bill will be replaced by '06' duplicate final bill
            billExtrDupTmpMapper.updateBillExtrDupTmp2(v_tmp_pid, pBillId);
            //siesta20210826 '01' demand-for-pay will be replaced by '05' duplicate demand-for-pay
            billExtrDupTmpMapper.updateBillExtrDupTmp3(v_tmp_pid, pBillId);
            String v_account_id = billExtrDupTmpMapper.selectAccountId(v_tmp_pid, pBillId);

            PersonEntity personEntity = personMapper.selectById(v_PERSON_ID);
            String v_IDV_BUS_SW = personEntity.getIdvBusSw();
            String v_LANG_CODE = personEntity.getLangCode();

            if ("FAX".equals(v_BILL_ROUTE_TYPE_CODE) || "EMAIL".equals(v_BILL_ROUTE_TYPE_CODE)) {
                v_address1 = "";
                v_address2 = "";
                v_address3 = "";
                v_address4 = "";
                /* Check if the Person is linked to the Account */
                //调用PA290_CALL_ACCT_PER_ROW_MAINT.BILL_EXTR_API_CONTER_PKG.bill_extr_api_CIPCACPR存储过程
                CustAccountEntity v_acct_per_row = accountMapper.selectOne(Wrappers.<CustAccountEntity>lambdaQuery()
                        .eq(CustAccountEntity::getAccountId, v_account_id).eq(CustAccountEntity::getPersonId, v_PERSON_ID));
                //调用FNC_RETRIEVE_BILL_ROW函数获取BILL主表信息
                BillEntity billEntity = billMapper.selectById(pBillId);
                //Using primary key to determine whether there is
                Ma031SetDefaultAddressBatchRequest request1 = new Ma031SetDefaultAddressBatchRequest();
                request1.setPiBillAddrSrceFlg(v_acct_per_row.getBillAddrSrcInd());
                request1.setPiProcessDttm(new Date());
                request1.setPiPerId(v_PERSON_ID);
                request1.setPiBillId(pBillId);
                request1.setPiAcctId(v_account_id);
                request1.setPiBillDt(String.valueOf(billEntity.getBillDt()));
                request1.setPiPerAcctLang(v_LANG_CODE);
                if (StringUtil.isNotEmpty(v_acct_per_row.getAccountId())) {
                    /* Handle the person under the same Account */
                    //调用MA031_SET_DEFAULT_ADDRESS存储过程设置默认地址
                    Ma031SetDefaultAddressBatchResponse ma031Response = ma031SetDefaultAddress(request1);
                    v_address1 = ma031Response.getPoAddressLine1();
                    v_address2 = ma031Response.getPoAddressLine2();
                    v_address3 = ma031Response.getPoAddressLine3();
                    v_address4 = ma031Response.getPoAddressLine4();
                } else {
                    //调用GC010_GET_ADDRESS_FROM_PER存储过程获取地址
                    Ma031SetDefaultAddressBatchResponse ma031Response = gc010GetAddressFromPer(request1);
                    v_address1 = ma031Response.getPoAddressLine1();
                    v_address2 = ma031Response.getPoAddressLine2();
                    v_address3 = ma031Response.getPoAddressLine3();
                    v_address4 = ma031Response.getPoAddressLine4();
                }
            }
            //调用SQ510_GET_SUFFIX.ZS690_SETUP_SQL_S_SUFFIX存储过程
            CustAccountEntity accountEntity = accountMapper.selectOne(new QueryWrapper<CustAccountEntity>().lambda()
                    .eq(CustAccountEntity::getAccountId, v_account_id)
                    .eq(CustAccountEntity::getPersonId, v_PERSON_ID));
            String v_name_pfx_sfx = accountEntity.getNamePrefxSuffx();
            String v_pfx_sfx_flg = accountEntity.getPrefxSuffxInd();
            if ("SX".equals(v_pfx_sfx_flg)) {
                v_name_pfx_sfx = v_name_pfx_sfx.trim();
                if (v_rc_name1.contains(v_name_pfx_sfx)) {
                    v_rc_name1 = v_rc_name1.replace(v_name_pfx_sfx, "");
                } else if (v_rc_name2.contains(v_name_pfx_sfx)) {
                    v_rc_name2 = v_rc_name2.replace(v_name_pfx_sfx, "");
                } else {
                    v_rc_name3 = v_rc_name3.replace(v_name_pfx_sfx, "");
                }
                if ("P".equals(v_IDV_BUS_SW)) {
                    //调用PA280_CALL_CMPBNMFX存储过程
                    v_rc_name1 = pa280CallCmpbnmfx(v_PERSON_ID, v_rc_name1);
                }
                if (StringUtil.isEmpty(v_rc_name2)) {
                    v_rc_name2 = v_name_pfx_sfx;
                } else {
                    if (StringUtil.isEmpty(v_rc_name3)) {
                        v_rc_name3 = v_name_pfx_sfx;
                    } else {
                        v_rc_name3 = v_rc_name3 + " " + v_name_pfx_sfx;
                        v_rc_name3 = StrUtils.padRight(v_rc_name3, 64, ' ');
                    }
                }
            } else {
                if ("P".equals(v_IDV_BUS_SW)) {
                    //调用PA280_CALL_CMPBNMFX存储过程
                    v_rc_name1 = pa280CallCmpbnmfx(v_PERSON_ID, v_rc_name1);
                }
            }
            billExtrDupTmpMapper.updateBillExtrDupTmp4(v_tmp_pid, pBillId, v_rc_name1);
            billExtrDupTmpMapper.updateBillExtrDupTmp5(v_tmp_pid, pBillId, v_rc_name2);
            billExtrDupTmpMapper.updateBillExtrDupTmp6(v_tmp_pid, pBillId, v_rc_name3);
            billExtrDupTmpMapper.updateBillExtrDupTmp7(v_tmp_pid, pBillId, v_address1);
            billExtrDupTmpMapper.updateBillExtrDupTmp8(v_tmp_pid, pBillId, v_address2);
            billExtrDupTmpMapper.updateBillExtrDupTmp9(v_tmp_pid, pBillId, v_address3);
            billExtrDupTmpMapper.updateBillExtrDupTmp10(v_tmp_pid, pBillId, v_address4);
            // see ct_detail_for_printer_key_0100
            double po_tot_amt_due = billExtrMapper.selectBillExtrDupTmpPoTotAmtDue(v_tmp_pid, pBillId);
            double po_autopay_amt = billExtrMapper.selectBillExtrDupTmpPoAutopayAmt(v_tmp_pid, pBillId);
            response.setPoTotAmtDue(po_tot_amt_due);
            response.setPoAutopayAmt(po_autopay_amt);
        }
        if (StringUtil.isEmpty(v_return_val)) {
            v_tmp_pid = v_tmp_pid;
        } else {
            v_tmp_pid = "";
        }
        response.setPoTmpPid(v_tmp_pid);
        return response;
    }

    public String aa000Main(Aa000MainDto aa000MainDto,
                            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                            List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
                            List<BitemWithReadAndQty> bitemReadList,
                            List<BitemCalcBdWithAttr> bitemCalcBdList,
                            List<FinTranWithAdj> finTranWithAdjList,
                            List<RateWithDtlAndCrit> rateList,
                            List<SvcDtlContractValEntity> contractValEntityList) {
        GlobalVariableBatch gvGlobalVariable = blobalVariableBatch.get();
        String piBatchMode = aa000MainDto.getPiBatchMode();
        String piDebugMode = aa000MainDto.getPiDebugMode();
        String piBillId = aa000MainDto.getPiBillId();
        int piSeqno = aa000MainDto.getPiSeqno();
        int piCopyCnt = aa000MainDto.getPiCopyCnt();
        String piExtractFileName = aa000MainDto.getPiExtractFileName();
        String piPrintedAndSent = aa000MainDto.getPiPrintedAndSent();
        String piUserId = aa000MainDto.getPiUserId();
        String piLanguageCd = aa000MainDto.getPiLanguageCd();
        Date piProcessDttm = aa000MainDto.getPiProcessDttm();
        int piBillCnt = aa000MainDto.getPiBillCnt();

        // 设置全局变量
        try {
            // 计算经过的秒数
            LocalDateTime now = LocalDateTime.now();
            int elapsedSeconds = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();
            gvGlobalVariable.setWElapseMin(elapsedSeconds);

            // 设置调试模式和批处理模式
            if (piDebugMode == null || piDebugMode.trim().isEmpty()) {
                gvGlobalVariable.setGvDebugMode("N");
            } else {
                gvGlobalVariable.setGvDebugMode(piDebugMode);
            }

            if (piBatchMode == null || piBatchMode.trim().isEmpty()) {
                gvGlobalVariable.setGvBatchMode(piBatchMode);
            } else {
                gvGlobalVariable.setGvBatchMode(piBatchMode);
            }

            // 查询账单状态
            String billStsInd = billExtrMapper.getBillStsIndByBillId(piBillId);
            if ("P".equals(billStsInd)) {
//                System.out.println("Pending bill will not be processed: " + piBillId);
                return "";
            }

            CheckInputBatchRequest request = new CheckInputBatchRequest();
            request.setBillId(piBillId);
            request.setSeqNo(piSeqno);
            request.setAlgParmVal1(aa000MainDto.getPiAlgParmVal1());
            request.setAlgParmVal2(aa000MainDto.getPiAlgParmVal2());
            request.setAlgParmVal3(aa000MainDto.getPiAlgParmVal3());
            request.setAlgParmVal4(aa000MainDto.getPiAlgParmVal4());
            request.setAlgParmVal5(aa000MainDto.getPiAlgParmVal5());
            request.setAlgParmVal6(aa000MainDto.getPiAlgParmVal6());
            request.setAlgParmVal7(aa000MainDto.getPiAlgParmVal7());
            request.setAlgParmVal8(aa000MainDto.getPiAlgParmVal8());
            request.setAlgParmVal9(aa000MainDto.getPiAlgParmVal9());
            request.setAlgParmVal10(aa000MainDto.getPiAlgParmVal10());
            request.setAlgParmVal11(aa000MainDto.getPiAlgParmVal11());
            request.setAlgParmVal12(aa000MainDto.getPiAlgParmVal12());
            request.setAlgParmVal13(aa000MainDto.getPiAlgParmVal13());
            request.setAlgParmVal14(aa000MainDto.getPiAlgParmVal14());
            request.setAlgParmVal15(aa000MainDto.getPiAlgParmVal15());
            request.setAlgParmVal16(aa000MainDto.getPiAlgParmVal16());
            request.setAlgParmVal17(aa000MainDto.getPiAlgParmVal17());
            request.setAlgParmVal18(aa000MainDto.getPiAlgParmVal18());
            request.setAlgParmVal19(aa000MainDto.getPiAlgParmVal19());
            request.setAlgParmVal20(aa000MainDto.getPiAlgParmVal20());
            request.setAlgParmVal21(aa000MainDto.getPiAlgParmVal21());
            request.setAlgParmVal22(aa000MainDto.getPiAlgParmVal22());
            request.setAlgParmVal23(aa000MainDto.getPiAlgParmVal23());
            request.setAlgParmVal24(aa000MainDto.getPiAlgParmVal24());
            request.setAlgParmVal25(aa000MainDto.getPiAlgParmVal25());
            request.setAlgParmVal26(aa000MainDto.getPiAlgParmVal26());
            request.setAlgParmVal27(aa000MainDto.getPiAlgParmVal27());
            request.setAlgParmVal28(aa000MainDto.getPiAlgParmVal28());
            request.setAlgParmVal29(aa000MainDto.getPiAlgParmVal29());
            //调用函数EA000_CHECK_INPUT
            CheckInputBatchResponse checkInputBatchResponse = ea000CheckInput(request);
            String errorCode = checkInputBatchResponse.getErrorCode() == null ? "" : checkInputBatchResponse.getErrorCode();
            String errorParam = checkInputBatchResponse.getErrorParam() == null ? "" : checkInputBatchResponse.getErrorParam();
            BillRoutingBatch billRouting1 = checkInputBatchResponse.getBillRouting();
            if (!"".equals(errorCode) || !"".equals(errorParam)) {
                //调用ZZ000_SETUP_SQL_S_CI_MSG.PA100_SUB_PARM_VALUES存储过程设置错误信息
                String v_error_msg = "";
                List<String> v_msg_parms = new ArrayList<>();
                v_msg_parms.add(errorParam);
                v_error_msg = pa100SubParmValues(errorCode, v_msg_parms);
                //调用ZZ000_SETUP_SQL_I_CI_MSG
                zz000SetupSqlICiMsg(aa000MainDto.getPiBillId(), "30", "EA000_CHECK_INPUT:" + v_error_msg);
                return "";
            }
            if ("".equals(gvGlobalVariable.getGvBatchMode())) {
                if ("ONLINE".equals(billRouting1.getBillRouteTypeCode())) {
                    gvGlobalVariable.setGvBatchMode("N");
                } else {
                    gvGlobalVariable.setGvBatchMode("Y");
                }
            }
            if ("Y".equals(gvGlobalVariable.getGvBatchMode()) && billRouting1.getExtrDate() != null) {
                if (gvGlobalVariable.getWErrorMsg() == null || gvGlobalVariable.getWErrorMsg().length() < 3000) {
                    gvGlobalVariable.setWErrorMsg(gvGlobalVariable.getWErrorMsg() + "|" + "AA000_MAIN ERROR: bill_routing.extr_date must = null for batch, extr_date=" + billRouting1.getExtrDate());
                }
            }

            // 执行主要逻辑
            Ga000PerformProcBatchRequest request2 = new Ga000PerformProcBatchRequest();
            request2.setPiUserId(piUserId);
            request2.setPiProcessDttm(piProcessDttm);
            request2.setPiLanguageCd(piLanguageCd);
            request2.setPiBillId(piBillId);
            request2.setPiSeqno(piSeqno);
            request2.setPiCopyCnt(piCopyCnt);
            request2.setPiBillCnt(piBillCnt);
            request2.setPiExtractFileName(piExtractFileName);
            request2.setPiBillRoutingRow(billRouting1);
            String poSkipBillSw = ga000PerformProc(
                request2, svcDtlWithTypeAndBalList, prevSvcDtlWithTypeAndBalList,
                bitemReadList, bitemCalcBdList, finTranWithAdjList, rateList, contractValEntityList);
            if (!Objects.equals(gvGlobalVariable.getWWarningMsg(), "")) {
                // 调用函数ZZ000_SETUP_SQL_I_CI_MSG
                zz000SetupSqlICiMsg(aa000MainDto.getPiBillId(), "20", gvGlobalVariable.getWWarningMsg());
            }
            if (!"".equals(gvGlobalVariable.getGvBatchMode())) {
                if ("ONLINE".equals(billRouting1.getBillRouteTypeCode())) {
                    gvGlobalVariable.setGvBatchMode("N");
                } else {
                    gvGlobalVariable.setGvBatchMode("Y");
                }
            }
            // 更新账单路由的提取日期
            if ("ONLINE".equals(billRouting1.getBillRouteTypeCode()) && billRouting1.getExtrDate() == null) {
                if (piSeqno != 1 || "Y".equals(piPrintedAndSent)) {
                    billExtrMapper.updateBillRoutingExtrDate(piBillId, piSeqno, piProcessDttm);
                }
            }
            //调用XQ051_DELETE_SA_LIST存储过程删除所有数据
//            billExtrSvcDtlGttMapper.delete(Wrappers.<BillExtrSvcDtlGttEntity>lambdaQuery());
            // 设置成功输出
            if ("E999999".equals(gvGlobalVariable.getGvOutputCode())) {
                gvGlobalVariable.setGvOutputCode("I000001");
                gvGlobalVariable.setGvOutputMsg("OK");
            }
            return poSkipBillSw;
        } catch (Exception e) {
            TRACER.error(e.getMessage(), e);
            // 异常处理
            gvGlobalVariable.setGvOutputCode("E999998");
            gvGlobalVariable.setGvOutputMsg("Exception occurred: " + e.getMessage());
            throw new RuntimeException("Exception occurred: " + e.getMessage());
        }
    }

    @Override
    public CheckInputBatchResponse ea000CheckInput(CheckInputBatchRequest request) {
        CheckInputBatchResponse response = new CheckInputBatchResponse();
        AlgorithmParameters params = algorithmParams.get();

        try {
            // 参数校验
            validateBasicParams(request, response);
            Pa020BillRtgRowMaintRequest requestRow = new Pa020BillRtgRowMaintRequest();
            requestRow.setPiBillId(request.getBillId());
            requestRow.setPiSeqno(request.getSeqNo());
            requestRow.setPiMustExist("Y");
            requestRow.setPiRowAction("R");
            /**
             * 必须要让Bill Routing有效。
             */
            BillRoutingBatch billRouting = pa020BillRtgRowMaint(requestRow);

            if (billRouting == null) {
                response.setErrorCode("256");
                response.setErrorParam("BILL_ROUTING");
                return response;
            }
            response.setBillRouting(billRouting);

            // 参数校验（示例部分参数）
            validateAlgorithmParams(request, params, response);

            return response;
        } catch (Exception e) {
            handleException(e, "CHECK_INPUT_ERROR");
            response.setErrorCode("SYSTEM_ERROR");
            response.setErrorParam(e.getMessage());
            return response;
        } finally {
            // algorithmParams.remove();
        }
    }

    @Override
    public BillRoutingBatch pa020BillRtgRowMaint(Pa020BillRtgRowMaintRequest pa020BillRtgRowMaintRequest) {
        BillExtrApiCipbblrrBatchRequest request = BeanUtil.copyProperties(pa020BillRtgRowMaintRequest, BillExtrApiCipbblrrBatchRequest.class);
        //调用存储过程
        return billExtrApiConterPkgService.billExtrApiCipbblrr(request);
    }

    public String ga000PerformProc(Ga000PerformProcBatchRequest request,
                                   List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                   List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
                                   List<BitemWithReadAndQty> bitemReadList,
                                   List<BitemCalcBdWithAttr> bitemCalcBdList,
                                   List<FinTranWithAdj> finTranWithAdjList,
                                   List<RateWithDtlAndCrit> rateList,
                                   List<SvcDtlContractValEntity> contractValEntityList) {
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        Ma010ExtractBillBatchRequest request1 = BeanUtil.copyProperties(request, Ma010ExtractBillBatchRequest.class);
        //调用MA010_EXTRACT_BILL存储过程
        Ma010ExtractBillBatchResponse ma010Response = ma010ExtractBill(request1,
            svcDtlWithTypeAndBalList, prevSvcDtlWithTypeAndBalList,
            bitemReadList, bitemCalcBdList,
            finTranWithAdjList,
            rateList, contractValEntityList);
        String v_skip_bill_sw = ma010Response.getPoSkipBillSw();
        CustAccountEntity v_acct_row = ma010Response.getPoAcctRow();
        BillEntity v_bill_row = ma010Response.getPoBillRow();
        Double v_amt_due = ma010Response.getPoAmtDue();
        Double v_apay_amt = ma010Response.getPoApayAmt();
        String v_od_prm_amt = ma010Response.getPoOdPrmAmt();
        String v_od_prm_date = ma010Response.getPoOdPrmDate();
        boolean isBatchModeN = "N".equals(gvGlobalVariableBatch1.getGv_batch_mode());
        boolean isBatchModeY = "Y".equals(gvGlobalVariableBatch1.getGv_batch_mode());
        boolean isDoNotExtrSwN = "N".equals(request.getPiBillRoutingRow().getDoNotExtrSw());
        boolean isSkipBillSwN = "N".equals(ma010Response.getPoSkipBillSw());
        boolean isVSkipBillSwN = "N".equals(v_skip_bill_sw);

        if ((isBatchModeN && isSkipBillSwN && isDoNotExtrSwN) ||
                (isBatchModeY && isVSkipBillSwN)) {
            //调用XQ037_CHK_BILL_SUMMARY.ZZ994_SETUP_SQL_S_BILL_SUMM存储过程
            Integer v_bill_summary_cnt = billSummaryMapper.selectCount(Wrappers.<BillSummaryEntity>lambdaQuery().eq(BillSummaryEntity::getBillId,
                    request.getPiBillId()));
            if (v_bill_summary_cnt == 0) {
                /* Perform Inser Bill Summary record to CM_BILL_SUMMARY */
                //调用XQ038_INS_BILL_SUMMARY.ZZ993_SETUP_SQL_I_BILL_SUMM存储过程插入BILL_SUMMARY表数据
                BillSummaryEntity billSummaryEntity = new BillSummaryEntity();
                billSummaryEntity.setAccountId(v_acct_row.getAccountId());
                billSummaryEntity.setBillId(v_bill_row.getBillId());
                billSummaryEntity.setDueDate(v_bill_row.getDueDt());
                billSummaryEntity.setBillIssuedDate(v_bill_row.getBillDt());
                billSummaryEntity.setTotAmtDue(BigDecimal.valueOf(v_amt_due));
                billSummaryEntity.setAutoPayAmt(BigDecimal.valueOf(v_apay_amt));
                billSummaryEntity.setOvd3P1(v_od_prm_amt);
                billSummaryEntity.setOvd3P2(v_od_prm_date);
                billSummaryEntity.setCreatedDate(new Date());
                billSummaryEntity.setCreatedBy("SYS");
                billSummaryEntity.setModifiedDate(new Date());
                billSummaryEntity.setModifiedBy("SYS");
                billSummaryEntity.setTimestamp(new Date());
                billSummaryEntity.setBillExtSrc(StrUtils.padRight(request.getPiBillRoutingRow().getBillRouteTypeCode(), 6, ' '));
                billSummaryMapper.insert(billSummaryEntity);
            } else {
                String w_stmt = billExtrMapper.getBillSummary(v_bill_row.getBillId());
                gvGlobalVariableBatch1.setW_stmt(w_stmt);
                SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMdd");
                SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String combinedStr = trim(v_acct_row.getAccountId()) + "," +
                        trim(v_bill_row.getBillId()) + "," +
                        dateFormat1.format(v_bill_row.getDueDt()) + "," +
                        dateFormat2.format(v_bill_row.getBillDt()) + "," +
                        v_amt_due + "," +
                        v_apay_amt + "," +
                        trim(v_od_prm_amt) + "," +
                        trim(v_od_prm_amt);
//                if (gvGlobalVariableBatch1.getW_stmt() != null && combinedStr != null) {
//                    if (!trim(gvGlobalVariableBatch1.getW_stmt()).equals(trim(combinedStr))) {
//                        String callSeq = "GA000_PERFORM_PROC BILL_SUMMARY mis-match:" + combinedStr;
//                        //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
//                        zz000SetupSqlICiMsg(v_bill_row.getBillId(), "10", callSeq);
//                    }
//                }
            }
        }
        return v_skip_bill_sw;
    }

    public Ma010ExtractBillBatchResponse ma010ExtractBill(
            Ma010ExtractBillBatchRequest request,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
            List<BitemWithReadAndQty> bitemReadList,
            List<BitemCalcBdWithAttr> bitemCalcBdList,
            List<FinTranWithAdj> finTranWithAdjList,
            List<RateWithDtlAndCrit> rateList,
            List<SvcDtlContractValEntity> contractValList) {
        Ma010ExtractBillBatchResponse response = new Ma010ExtractBillBatchResponse();
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        BillRoutingBatch v_bill_routing_row = request.getPiBillRoutingRow();
        //调用MA020_MAIN_BILL_DETAILS存储过程
        Ma020MainBillDetailsBatchResponse ma020Response = ma020MainBillDetails(
                request.getPiBillId(), request.getPiSeqno(), request.getPiLanguageCd(),
                svcDtlWithTypeAndBalList, bitemReadList);
        response.setPoSkipBillSw(ma020Response.getPoSkipBillSw());

        String v_bill_type;
        String v_acct_chk_digit;
        if ("N".equals(gvGlobalVariableBatch1.getGv_batch_mode()) || ("Y".equals(gvGlobalVariableBatch1.getGv_batch_mode()) && "N".equals(ma020Response.getPoSkipBillSw()))) {

            Ma030PrepareMainExtractBatchRequest ma030Request = new Ma030PrepareMainExtractBatchRequest();
            ma030Request.setPiUserId(request.getPiUserId());
            ma030Request.setPiProcessDttm(request.getPiProcessDttm());
            ma030Request.setPiSeqno(request.getPiSeqno());
            ma030Request.setPiAcctRow(ma020Response.getPoAcctRow());
            ma030Request.setPiBillRow(ma020Response.getPoBillRow());
            ma030Request.setPiBillRoutingRow(v_bill_routing_row);
            ma030Request.setPiPerRow(ma020Response.getPoPerRow());
            ma030Request.setPiCustClRow(ma020Response.getPoCustClRow());
            ma030Request.setPiLanguageCd(ma020Response.getPoLanguageCd());
            ma030Request.setPiEntityName(ma020Response.getPoMainPerEntityName());
            ma030Request.setPiFinalBillSw(ma020Response.getPoFinalBillSw());
            ma030Request.setPiNonconSw(ma020Response.getPoNonconSw());
            ma030Request.setPiWithAutopaySw(ma020Response.getPoWithAutopaySw());
            ma030Request.setPiAutopayAmt(ma020Response.getPoAutopayAmt());
            ma030Request.setPiExcessAmt(ma020Response.getPoExcessAmt());
            ma030Request.setPiMainPerId(ma020Response.getPoMainPerId());
            ma030Request.setPiSkipBillSw(ma020Response.getPoSkipBillSw());
            //调用函数MA030_PREPARE_MAIN_EXTRACT
            Ma030PrepareMainExtractBatchResponse ma030Response = ma030PrepareMainExtract(ma030Request, ma020Response.getGv_bill_sort_type());
            response.setPoApayAmt(ma030Response.getPoApayAmt());
            response.setPoAmtDue(ma030Response.getPoAmtDue());
            response.setPoOdPrmAmt(ma030Response.getPoOdPrmAmt());
            response.setPoOdPrmDate(ma030Response.getPoOdPrmDate());
            v_bill_type = ma030Response.getPoBillType();
            v_acct_chk_digit = ma030Response.getPoAcctChkDigit();

            Ma040PaymentRecBatchRequest ma040Request = new Ma040PaymentRecBatchRequest();
            ma040Request.setPiUserId(request.getPiUserId());
            ma040Request.setPiProcessDttm(request.getPiProcessDttm());
            ma040Request.setPiBillRow(ma020Response.getPoBillRow());
            ma040Request.setPiBillCnt(request.getPiBillCnt());
            ma040Request.setPiInstamtInstAmt(BigDecimal.valueOf(ma020Response.getPoInstAmt()));
            ma040Request.setPiBillType(v_bill_type);
            ma040Request.setPiAcctIdWithCheckDigit(v_acct_chk_digit);
            ma040Request.setPiCustClRow(ma020Response.getPoCustClRow());
            ma040Request.setPiFinalBillSw(ma020Response.getPoFinalBillSw());
            ma040Request.setPiNonconSw(ma020Response.getPoNonconSw());
            //调用MA040_PAYMENT_REC存储过程
            ma040PaymentRec(ma040Request, finTranWithAdjList, bitemReadList, ma020Response.getGv_bill_sort_type());
            /* Prepare the details on bill messages 准备账单信息的详细信息 */
            //调用MA050_BILL_MESSAGES存储过程
            ma050BillMessages(request.getPiUserId(), request.getPiProcessDttm(), ma020Response.getPoBillRow(),
                    request.getPiBillId(), ma020Response.getPoLanguageCd());
            //调用MD010_GET_NOS_BILLMSG存储过程
            Md010GetNosBillmsgBatchResponse md010Response = md010GetNosBillmsg(request.getPiUserId(), request.getPiProcessDttm(), ma020Response.getPoBillRow(), request.getPiBillId(), ma020Response.getPoLanguageCd());
            /* Prepare the details on charges and adjustments (by prem/sa) */
            //调用MC010_PREMISE_GROUP存储过程
            Mc010PremiseGroupBatchRequest mc010Request = new Mc010PremiseGroupBatchRequest();
            mc010Request.setPiUserId(request.getPiUserId());
            mc010Request.setPiProcessDttm(request.getPiProcessDttm());
            mc010Request.setPiBillRow(ma020Response.getPoBillRow());
            mc010Request.setPiBillRoutingRow(v_bill_routing_row);
            mc010Request.setPiBillId(request.getPiBillId());
            mc010Request.setPiLanguageCd(ma020Response.getPoLanguageCd());
            mc010Request.setPiDsddfBmsgCd(md010Response.getDsddfBitemCd());
            mc010Request.setPiTesrateBmsgCd(md010Response.getTesrateBitemCd());
            mc010Request.setPiTesdfBmsgCd(md010Response.getTesdfBitemCd());
            mc010Request.setPiDfbitemCd(md010Response.getDfbitemCd());
            mc010Request.setPiBillType(v_bill_type);
            mc010Request.setPiPrebillId(ma020Response.getPoPrebillId());
            mc010PremiseGroup(mc010Request, svcDtlWithTypeAndBalList,
                prevSvcDtlWithTypeAndBalList, bitemReadList, bitemCalcBdList,
                finTranWithAdjList,
                rateList, contractValList);
            /* Prepare the record to designate the end of a bill. */
            //调用MZ010_END_OF_BILL存储过程
            mz010EndOfBill(request.getPiUserId(), request.getPiProcessDttm(), ma020Response.getPoBillRow());
            response.setPoAcctRow(ma020Response.getPoAcctRow());
            response.setPoBillRow(ma020Response.getPoBillRow());
        }
        return response;
    }

    public Ma020MainBillDetailsBatchResponse ma020MainBillDetails(
            String piBillId, Integer piSeqno, String piLanguageCd,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<BitemWithReadAndQty> bitemList) {
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        String v_skip_bill_sw = "N";
        Ma020MainBillDetailsBatchResponse response = new Ma020MainBillDetailsBatchResponse();
        //调用FNC_RETRIEVE_BILL_ROW函数获取BILL主表信息
        BillEntity billEntity = billMapper.selectById(piBillId);
        String accountId = billEntity.getAccountId();
        Date billDate = billEntity.getBillDt();

        // 将Date转换为LocalDateTime
        LocalDateTime localDateTime = billDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        // 定义日期格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        // 将LocalDateTime转换为String类型
        String dateStr = localDateTime.format(formatter);
        //调用FNC_RETRIEVE_ACCOUNT_ROW函数获取ACCOUNT主表信息
        CustAccountEntity accountEntity = accountMapper.selectById(accountId);
        //调用FNC_RETRIEVE_CUST_ROW函数获取CFG_CUST_CLS表信息
        CfgCustTypeEntity cfgCustTypeEntity = cfgCustTypeMapper.selectById(accountEntity.getCustClsCode());
        //调用PA050_MAIN_PERSON函数获取ACCOUNT表的PERSON_ID信息
        String poPerId = accountEntity.getPersonId();
        //调用PA060_MAIN_PERSON_NAME函数获取PERSON_NAME表的SEQ, PST_NAME信息
        List<PersonNameEntity> personNameEntities = personNameMapper.selectList(new QueryWrapper<PersonNameEntity>().lambda()
                .eq(PersonNameEntity::getPersonId, poPerId)
                .eq(PersonNameEntity::getMainNameSw, "Y"));
        PersonNameEntity personName = personNameEntities.getFirst();
        int v_main_per_name_seq_num = personName.getSeq();//未使用
        String v_main_per_name_entity_name = personName.getPstName();
        //调用SQ800_GET_ACCTCH函数中调用ZS910_SETUP_SQL_S_ACCTCH函数获取po_attr_val
        String attrVal = zs910SetupSqlSAcctch(accountId, "ACCTLANG", dateStr);
        //调用PA070_PERSON_ROW_MAINT存储过程中的bill_extr_api_CIPCPERR储存过程获取PERSON表信息
        PersonEntity personEntity = personMapper.selectById(poPerId);
//        String langCode = personEntity.getLangCode();
        String v_language_cd = "";
//        if (com.alibaba.excel.util.StringUtils.isEmpty(attrVal)) {
//            v_language_cd = langCode;
//        } else {
//            v_language_cd = attrVal;
//        }
//        if (!com.alibaba.excel.util.StringUtils.isEmpty(piLanguageCd)) {
//            v_language_cd = piLanguageCd;
//        }
        v_language_cd = attrVal;
        //调用SQ060_GET_BILLAPY存储过程中的ZS150_SETUP_SQL_S_BILLAPY获取BILL_MSG_PARAM表中SEQ = 1，2的 MSG_PARAM_VAL的信息
        Zs150SetupSqlSBillapyBatchResponse auto = zs150SetupSqlSBillapy(piBillId, "AUTO");//po_autopay_amt,po_excess_amt,po_with_autopay_sw
        //调用SQ020_GET_SPCNT存储过程中的ZS210_SETUP_SQL_S_SPCNT获取po_with_sd，po_with_bitem，po_wout_bitem信息
        Zs210SetupSqlSSpcntBatchResponse entity = zs210SetupSqlSSpcnt(piBillId, svcDtlWithTypeAndBalList, bitemList);
        if (entity == null) {
            entity = new Zs210SetupSqlSSpcntBatchResponse();
            entity.setPoWithSd(0);
            entity.setPoWithBitem(0);
            entity.setPoWoutBitem(0);
        }
        // 检查 v_spcnt_with_pre_cnt
        if (entity.getPoWithSd() > 99) {
            entity.setPoWithSd(4);
        }

        // 检查 v_spcnt_with_bitem_cnt
        if (entity.getPoWithBitem() > 99) {
            entity.setPoWithBitem(Integer.parseInt(String.valueOf(entity.getPoWithBitem()).substring(String.valueOf(entity.getPoWithBitem()).length() - 2)));
        }

        // 检查 v_spcnt_wout_bitem
        if (entity.getPoWoutBitem() > 99) {
            entity.setPoWoutBitem(Integer.parseInt(String.valueOf(entity.getPoWoutBitem()).substring(String.valueOf(entity.getPoWoutBitem()).length() - 2)));

        }

        // 计算 v_bill_pre_cnt
        int v_bill_pre_cnt = 0;
        if (entity.getPoWoutBitem() > 0) {
            v_bill_pre_cnt = entity.getPoWithSd() + entity.getPoWithBitem() + 1;
        } else {
            v_bill_pre_cnt = entity.getPoWithSd() + entity.getPoWithBitem();
        }
        if (v_bill_pre_cnt > 17) {
//            gv_bill_sort_type = "9";// B-MANUAL
            response.setGv_bill_sort_type("9");
        } else {
//            gv_bill_sort_type = "5";// B-OTHERS
            response.setGv_bill_sort_type("5");
            //调用SQ030_GET_SORTTY存储过程中的ZS220_SETUP_SQL_S_SORTTY存储过程获取数据列表
            List<Integer> integers = zs220SetupSqlSSortty(piBillId);
            // 遍历游标结果
            for (Integer v_sortty_period : integers) {
                if (!response.getGv_bill_sort_type().equals("1")) { // B-4-MONTHLY
                    if (v_sortty_period == 3) {
//                        gv_bill_sort_type = "1"; // B-4-MONTHLY
                        response.setGv_bill_sort_type("1");
                    }
                    if (v_sortty_period == 12) {
//                        gv_bill_sort_type = "2"; // B-MONTHLY
                        response.setGv_bill_sort_type("2");
                    }
                }
            }
        }
        /* Bill's balance */
        //调用fncCalcPayableAmount函数计算应付金额
        BigDecimal bigDecimal = fncCalcPayableAmount(billEntity.getBillId(),
            params.getDepositAdjCd(), params.getInstFldName(),
            params.getDisputeFldName(), params.getOverpaySdType());
        // NOTE
        bigDecimal = fncPayableAmountSpecialHandle(billEntity.getBillId(), bigDecimal,
            BigDecimal.valueOf(params.getMaxAmtDue() == null ? 0.0 : params.getMaxAmtDue()),
            params.getFinalBillMsgCd(), params.getInstFldName(), params.getDisputeFldName());
        if (bigDecimal.compareTo(BigDecimal.ZERO) < 1) {
            gvGlobalVariableBatch1.setW_tot_amt(0.0);
        } else {
            gvGlobalVariableBatch1.setW_tot_amt(bigDecimal.doubleValue());
        }

        // 调用SQ780_GET_CURCHG函数获取v_cur_charges值  目前没有其他地方使用

        //调用函数FNC_IS_FINAL_BILL.SQ100_GET_FINAL_BILL.ZS190_SETUP_SQL_S_FNLBM给gv_alg_params.final_bill_msg_cd赋值
        Integer i = zs190SetupSqlSFnlbm(piBillId, params.getFinalBillMsgCd());
        String v_final_bill_sw = "N";
        if (i > 0) {
            v_final_bill_sw = "Y";
        }
        //调用函数SQ300_GET_INSTAMT.ZS390_SETUP_SQL_S_INSTAMT获取v_inst_amt值
        Double v_inst_amt = zs390SetupSqlSInstamt(accountId, piBillId, params.getInstFldName());
        //调用SQ610_GET_PREBILL.ZS720_SETUP_SQL_S_PREBILL获取 v_prebill_id 值
        BillEntity billEntity1 = zs720SetupSqlSPrebill(accountId, billDate, billEntity.getCmpltDt());
        String v_prebill_id = "";
        if (billEntity1 != null) {
            v_prebill_id = billEntity1.getBillId();
        } else {
            v_prebill_id = null;
        }
        //调用函数FNC_IS_NON_CONSUMPTION.SQ500_GET_NONCON.ZS630_SETUP_SQL_S_NONCON获取v_noncon_sw的值
        String v_noncon_sw = zs630SetupSqlSNoncon(piBillId, params.getInstFldName(), params.getDisputeFldName());
        String v_condition_ebill_sw = "N";
        //调用XQ032_CHECK_BILL_ROUTING.ZS970_SETUP_SQL_S_BILL_RT
        Integer v_bill_rt_cnt = zs970SetupSqlSBillRt(piBillId, "EMAIL", piSeqno);
        if (v_bill_rt_cnt == 1) {
            v_condition_ebill_sw = "Y";
        }
        if ("N".equals(v_condition_ebill_sw)) {
            //XQ033_CHECK_ESD_ROUTING.ZS980_SETUP_SQL_S_ESD_RT
            v_bill_rt_cnt = zs980SetupSqlSEsdRt(piBillId, piSeqno);
            if (v_bill_rt_cnt == 1) {
                v_condition_ebill_sw = "Y";
            }
        }
        Integer v_final_bill_cnt = zs190SetupSqlSFnlbm(piBillId, "ZX01");
        String v_mrr_zx_sw = "";
        if (v_final_bill_cnt > 0) {
            v_mrr_zx_sw = "Y";
        } else {
            v_mrr_zx_sw = "N";
        }
        String v_condition_1_sw = "N";
        String v_condition_2_sw = "N";
        String v_condition_3_sw = "N";
        //调用MP001_SKIP_BILL_0 存储过程 给v_skip_bill_sw赋值
        v_skip_bill_sw = mp001SkipBill0(piBillId, v_final_bill_sw, v_condition_ebill_sw, v_skip_bill_sw, v_main_per_name_entity_name);
        v_condition_1_sw = mp010SkipBill1(v_final_bill_sw, accountId, billDate, v_condition_1_sw);
        if ("Y".equals(v_condition_1_sw)) {
            //调用MP040_UPDATE_SUPP_IND.SQ470_UPDATE_SUPPIND.ZS560_SETUP_SQL_U_SUPPIND存储过程更新供应商信息(CUST_ACCT_PROP)
            zs560SetupSqlUSuppind(accountId, params.getIgnoreNextCd(), billDate);
            //调用MP030_SKIP_BILL_3存储过程
            v_condition_3_sw = mp030SkipBill3(piBillId, v_condition_3_sw);
            if ("Y".equals(v_condition_3_sw)) {
                v_skip_bill_sw = "Y";
            }
        } else {
            //调用MP020_SKIP_BILL_2.SQ790_GET_EXTFLG存储过程
            Double wTotAmt = gvGlobalVariableBatch1.getW_tot_amt();
            Double maxAmtDue = params.getMaxAmtDue() == null ? 0.0 : params.getMaxAmtDue();
            if (wTotAmt > maxAmtDue) {
                v_condition_2_sw = "Y";
            } else {
                v_condition_2_sw = "N";
            }
            if ("Y".equals(v_condition_2_sw)) {
                v_skip_bill_sw = "Y";
                if (v_condition_ebill_sw.equals("Y")) {
                    v_skip_bill_sw = "N";
                }
                if (v_mrr_zx_sw.equals("Y")) {
                    v_skip_bill_sw = "N";
                }
            } else {
                //调用MP030_SKIP_BILL_3
                v_condition_3_sw = mp030SkipBill3(piBillId, v_condition_3_sw);
                if (v_condition_3_sw.equals("Y")) {
                    v_skip_bill_sw = "Y";
                }
            }
        }
        if (v_skip_bill_sw.equals("Y")) {
            //调用XQ044_CHECK_BILL_LOW_CONS_MSG.ZZ995_SETUP_SQL_S_BILLLCM获取v_bill_lcm_cnt值
            Integer v_bill_lcm_cnt = zz995SetupSqlSBilllcm(piBillId);
            if (v_bill_lcm_cnt > 0) {
                v_skip_bill_sw = "N";
            }
        }
        if (v_condition_ebill_sw.equals("Y")) {
            v_skip_bill_sw = ma021SkipBillCheck(piBillId, v_skip_bill_sw);
        }
        if (gvGlobalVariableBatch1.getGv_batch_mode().equals("Y")) {
            String piAttrTypeCode = "SKIPBILL";
            //XQ010_CHECK_SKIP_BILL_CHAR.ZS930_SETUP_SQL_S_BILLCH获取seq_num值
            Integer v_billch_seq_num = zs930SetupSqlSBillch(piBillId, piAttrTypeCode);
            String v_billch_data_found = "Y";
            if (v_billch_seq_num == null || v_billch_seq_num == 0) {
                v_billch_data_found = "N";
            }
            //调用XQ020_SETUP_SKIP_BILL_CHAR.ZS920_SETUP_SQL_I_BILLCH插入数据到BILL_ATTR表
            if (v_billch_data_found.equals("N")) {
                //插入
                zs920SetupSqlIBillchInsert(piBillId, piAttrTypeCode, v_skip_bill_sw);
            } else {
                //更新
                zs920SetupSqlIBillchUpdate(piBillId, piAttrTypeCode, v_skip_bill_sw, v_billch_seq_num);
            }
        }
        response.setPoAcctRow(accountEntity);
        response.setPoBillRow(billEntity);
        response.setPoPerRow(personEntity);
        response.setPoCustClRow(cfgCustTypeEntity);
        response.setPoLanguageCd(v_language_cd);
        response.setPoFinalBillSw(v_final_bill_sw);
        response.setPoNonconSw(v_noncon_sw);
        response.setPoWithAutopaySw(auto.getPoWithAutopaySw());
        response.setPoAutopayAmt(auto.getPoAutopayAmt());
        response.setPoExcessAmt(auto.getPoExcessAmt());
        response.setPoMainPerId(poPerId);
        response.setPoMainPerEntityName(v_main_per_name_entity_name);
        response.setPoSkipBillSw(v_skip_bill_sw);
        response.setPoInstAmt(v_inst_amt);
        response.setPoPrebillId(v_prebill_id);
        return response;
    }

    @Override
    public String zs910SetupSqlSAcctch(String acctId, String attrType, String piBusDate) {
        return billExtrMapper.getAttrVal(acctId, attrType, piBusDate);
    }

    @Override
    public Zs150SetupSqlSBillapyBatchResponse zs150SetupSqlSBillapy(String piBillId, String piBillMsgCd) {
        //BILL_MSG_PARAM表中SEQ = 1，2的 MSG_PARAM_VAL的信息BILL_MSG_PARAM表删除，pi_bill_msg_cd = 'AUTO'
        Zs150SetupSqlSBillapyBatchResponse res = billExtrMapper.zs150SetupSqlSBillapy(piBillId, piBillMsgCd);
        if (res == null) {
            res = new Zs150SetupSqlSBillapyBatchResponse();
            res.setPoAutopayAmt(0.00);
            res.setPoExcessAmt(0.00);
            res.setPoWithAutopaySw("N");
            return res;
        }
        res.setPoWithAutopaySw("Y");
        return res;
    }

    private Zs210SetupSqlSSpcntBatchResponse zs210SetupSqlSSpcnt(
            String piBillId,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<BitemWithReadAndQty> bitemList) {
        Zs210SetupSqlSSpcntBatchResponse retVal = new Zs210SetupSqlSSpcntBatchResponse();
        retVal.setPoWithSd(svcDtlWithTypeAndBalList.size());
        int withBitem = 0;
        int withoutBitem = 0;
        for (BitemWithReadAndQty bitem : bitemList) {
            boolean found = false;
            for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
                if (svc.getSvcId().equals(bitem.getSvcId())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                withBitem++;
            }
        }
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            boolean found = false;
            for (BitemWithReadAndQty bitem : bitemList) {
                if (svc.getSvcId().equals(bitem.getSvcId())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                withoutBitem++;
            }
        }
        retVal.setPoWithBitem(withBitem);
        retVal.setPoWoutBitem(withoutBitem);
        return retVal;
//        return billExtrMapper.zs210SetupSqlSSpcnt(piBillId);
    }

    @Override
    public List<Integer> zs220SetupSqlSSortty(String piBillId) {
        return billExtrMapper.zs220SetupSqlSSortty(piBillId);
    }

    @Override
    public Integer zs190SetupSqlSFnlbm(String piBillId, String piBillMsgCd) {
        return billExtrMapper.zs190SetupSqlSFnlbm(piBillId, piBillMsgCd);
    }

    @Override
    public Double zs390SetupSqlSInstamt(String piAcctId, String piBillId, String piFieldName) {
        // 表SVC_DTL_RCHG_HIST没有进行迁移，去掉了子查询
        BigDecimal retVal = billExtrMapper.zs390SetupSqlSInstamt(piAcctId, piBillId, piFieldName);
        return retVal.doubleValue();
    }

    @Override
    public BillEntity zs720SetupSqlSPrebill(String piAcctId, Date piBillDt, Date piCompleteDttm) {
        return billExtrMapper.zs720SetupSqlSPrebill(piAcctId, piBillDt, piCompleteDttm);
    }

    @Override
    public String zs630SetupSqlSNoncon(String piBillId, String piFieldName1, String piFieldName2) {
        return billExtrMapper.zs630SetupSqlSNoncon(piBillId, piFieldName1, piFieldName2);
    }

    @Override
    public Integer zs970SetupSqlSBillRt(String piBillId, String piBillRteTypeCd, Integer piBillSeqNo) {
        return billExtrMapper.zs970SetupSqlSBillRt(piBillId, piBillRteTypeCd, piBillSeqNo);
    }

    @Override
    public Integer zs980SetupSqlSEsdRt(String piBillId, Integer piBillSeqNo) {
        return billExtrMapper.zs980SetupSqlSEsdRt(piBillId, piBillSeqNo);
    }

    @Override
    public String mp001SkipBill0(String piBillId, String piFinalBillSw, String piConditionEbillSw, String piSkipBillSw, String piEntityName) {
        AlgorithmParameters params = algorithmParams.get();
        if ("Y".equals(piFinalBillSw)) {
            //调用XQ030_CHECK_FINAL_BILL.ZS930_SETUP_SQL_S_2ND_FIN获取v_flg值
            Integer v_flg = zs940SetupSqlS2ndfin(piBillId);
            String po_2nd_final_bill = "N";
            if (v_flg == 0) {
                po_2nd_final_bill = "Y";
            }
            //调用XQ031_CHECK_FINAL_BILL_AMT.ZS950_SETUP_SQL_S_FINAMT获取v_final_bill_tot_amt值
            Double v_final_bill_tot_amt = zs950SetupSqlSFinamt(piBillId, params.getInstFldName(), params.getDisputeFldName());
            if ("Y".equals(po_2nd_final_bill) && v_final_bill_tot_amt < 1) {
                piSkipBillSw = "Y";
            }
            String v_housing_sw = "";
            if (piEntityName.toUpperCase().indexOf("HOUSING") > 0) {
                v_housing_sw = "Y";
            } else {
                v_housing_sw = "N";
            }
            if ("Y".equals(v_housing_sw) && v_final_bill_tot_amt < 0.1) {
                piSkipBillSw = "Y";
                if ("Y".equals(piConditionEbillSw)) {
                    piSkipBillSw = "N";
                }
            }
        }
        return piSkipBillSw;
    }

    @Override
    public Integer zs940SetupSqlS2ndfin(String piBillId) {
        return billExtrMapper.zs940SetupSqlS2ndfin(piBillId);
    }

    @Override
    public Double zs950SetupSqlSFinamt(String piBillId, String piFieldName1, String piFieldName2) {
        return billExtrMapper.zs950SetupSqlSFinamt(piBillId, piFieldName1, piFieldName2);
    }

    @Override
    public String mp010SkipBill1(String piFinalBillSw, String piAcctId, Date piBillDt, String piCondition1Sw) {
        AlgorithmParameters params = algorithmParams.get();
        String po_condition_1_sw = piCondition1Sw;
        if ("Y".equals(piFinalBillSw)) {
            po_condition_1_sw = "Y";
        } else {
            //调用SQ460_GET_CNTSUPP.ZS550_SETUP_SQL_S_CNTSUPP获取v_supp_cnt值
            Integer v_supp_cnt = zs550SetupSqlSCntsupp(piAcctId, params.getIgnorePrintCd(), params.getIgnoreNextCd(), piBillDt);
            if (v_supp_cnt > 0) {
                po_condition_1_sw = "Y";
            }
        }
        return po_condition_1_sw;
    }

    @Override
    public Integer zs550SetupSqlSCntsupp(String piAcctId, String piAttrTypeCd1, String piAttrTypeCd2, Date piBillDt) {
        return billExtrMapper.zs550SetupSqlSCntsupp(piAcctId, piAttrTypeCd1, piAttrTypeCd2, piBillDt);
    }

    @Override
    public void zs560SetupSqlUSuppind(String piAcctId, String piAttrTypeCd, Date piBillDt) {
        billExtrMapper.zs560SetupSqlUSuppind(piAcctId, piAttrTypeCd, piBillDt);
    }

    @Override
    public String mp030SkipBill3(String piBillId, String piCondition3Sw) {
        //调用SQ480_GET_USAGE.ZS570_SETUP_SQL_S_USAGE存储过程
        Integer v_cnt = zs570SetupSqlSUsage();
        if (v_cnt > 0) {
            piCondition3Sw = "Y";
            //ZZ000_SETUP_SQL_I_CI_MSG存储过程生成全局异常
            zz000SetupSqlICiMsg(piBillId, "30", "MP030_SKIP_BILL_3 error 11141 Account has more than one SP:");
        }
        return piCondition3Sw;
    }

    @Override
    public Integer zs570SetupSqlSUsage() {
        return billExtrMapper.zs570SetupSqlSUsage();
    }

    @Override
    public Integer zz995SetupSqlSBilllcm(String piBillId) {
        return billExtrMapper.zz995SetupSqlSBilllcm(piBillId);
    }

    @Override
    public String ma021SkipBillCheck(String piBillId, String vSkipBillSw) {
        if ("N".equals(vSkipBillSw)) {
            //XQ039_CHK_BSEG.ZS995_SETUP_SQL_C_BSEG存储过程
            Integer v_chk_bitem_cnt = zs995SetupSqlCBseg(piBillId);
            //XQ040_CHK_ADJ.ZS996_SETUP_SQL_C_ADJ存储过程
            Integer v_chk_adj_cnt = zs996SetupSqlCAdj(piBillId);
            //XQ041_CHK_ADJ_UD.ZS997_SETUP_SQL_C_ADJ_UD存储过程
            Integer v_chk_adj_ud_cnt = zs997SetupSqlCAdjUd(piBillId);
            //XQ043_CHK_ADJ_UD_N
            Integer v_chk_adj_ud_n_cnt = zs999SetupSqlCAdjUdN(piBillId);
            if (v_chk_bitem_cnt == 0 && v_chk_adj_cnt == 0) {
                vSkipBillSw = "Y";
            } else {
                if (v_chk_bitem_cnt == 0 && v_chk_adj_ud_cnt >= 1 && v_chk_adj_ud_n_cnt == 0) {
                    vSkipBillSw = "Y";
                }
            }
        }
        return vSkipBillSw;
    }

    @Override
    public Integer zs995SetupSqlCBseg(String piBillId) {
        return billExtrMapper.zs995SetupSqlCBseg(piBillId);
    }

    @Override
    public Integer zs996SetupSqlCAdj(String piBillId) {
        return billExtrMapper.zs996SetupSqlCAdj(piBillId);
    }

    @Override
    public Integer zs997SetupSqlCAdjUd(String piBillId) {
        return billExtrMapper.zs997SetupSqlCAdjUd(piBillId);
    }

    @Override
    public Integer zs999SetupSqlCAdjUdN(String piBillId) {
        return billExtrMapper.zs999SetupSqlCAdjUdN(piBillId);
    }

    @Override
    public Integer zs930SetupSqlSBillch(String piBillId, String piAttrTypeCode) {
        return billExtrMapper.zs930SetupSqlSBillch(piBillId, piAttrTypeCode);
    }

    @Override
    public void zs920SetupSqlIBillchInsert(String piBillId, String piAttrTypeCode, String piAttrVal) {
        billExtrMapper.zs920SetupSqlIBillchInsert(piBillId, piAttrTypeCode, piAttrVal);
    }

    @Override
    public void zs920SetupSqlIBillchUpdate(String piBillId, String piAttrTypeCode, String piAttrVal, Integer piSeqNum) {
        billExtrMapper.zs920SetupSqlIBillchUpdate(piBillId, piAttrTypeCode, piAttrVal, piSeqNum);
    }

    public Ma030PrepareMainExtractBatchResponse ma030PrepareMainExtract(Ma030PrepareMainExtractBatchRequest request,
                                                                        String gv_bill_sort_type) {
        Ma030PrepareMainExtractBatchResponse ma030response = new Ma030PrepareMainExtractBatchResponse();
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("0100");
        gvBillExtractionHeaderBatch1.setSortKeyBillId(request.getPiBillRow().getBillId());
        String v_bp_extr_acct_id = request.getPiBillRow().getAccountId();
        String v_currency_symbol = "$";
        gvBillExtractionHeaderBatch1.setSortKeyBillId(request.getPiBillRoutingRow().getBillId());
        gvBillExtractionHeaderBatch1.setSortKeyPerId(request.getPiBillRoutingRow().getPersonId());
        gvBillExtractionHeaderBatch1.setSortKeySeqno(request.getPiBillRoutingRow().getSeq());
        gvBillExtractionHeaderBatch1.setMailingKeyPostal(" ");
        String v_bp_extr_entity_name1 = request.getPiBillRoutingRow().getNameLine1();
        String v_bp_extr_entity_name2 = request.getPiBillRoutingRow().getNameLine2();
        String v_bp_extr_entity_name3 = request.getPiBillRoutingRow().getNameLine3();
        String v_bill_extr_address1 = request.getPiBillRoutingRow().getAddrLine1();
        String v_bill_extr_address2 = request.getPiBillRoutingRow().getAddrLine2();
        String v_bill_extr_address3 = request.getPiBillRoutingRow().getAddrLine3();
        String v_bill_extr_address4 = request.getPiBillRoutingRow().getAddrLine4();
        v_bp_extr_entity_name1 = v_bp_extr_entity_name1 == null ? "" : v_bp_extr_entity_name1;
        v_bp_extr_entity_name2 = v_bp_extr_entity_name2 == null ? "" : v_bp_extr_entity_name2;
        v_bp_extr_entity_name3 = v_bp_extr_entity_name3 == null ? "" : v_bp_extr_entity_name3;
        if ("FAX".equals(request.getPiBillRoutingRow().getBillRouteTypeCode()) || "EMAIL".equals(request.getPiBillRoutingRow().getBillRouteTypeCode())) {
            v_bill_extr_address1 = "";
            v_bill_extr_address2 = "";
            v_bill_extr_address3 = "";
            v_bill_extr_address4 = "";
            //调用PA290_CALL_ACCT_PER_ROW_MAINT.BILL_EXTR_API_CONTER_PKG.bill_extr_api_CIPCACPR存储过程
            CustAccountEntity v_acct_per_row = accountMapper.selectById(v_bp_extr_acct_id);

            Ma031SetDefaultAddressBatchRequest request1 = new Ma031SetDefaultAddressBatchRequest();
            request1.setPiBillAddrSrceFlg(v_acct_per_row.getBillAddrSrcInd());
            request1.setPiProcessDttm(request.getPiProcessDttm());
            request1.setPiPerId(request.getPiBillRoutingRow().getPersonId());
            request1.setPiBillId(request.getPiBillRoutingRow().getBillId());
            request1.setPiAcctId(v_acct_per_row.getAccountId());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            request1.setPiBillDt(sdf.format(request.getPiBillRow().getBillDt()));
            request1.setPiPerAcctLang(request.getPiPerRow().getLangCode());
            Ma031SetDefaultAddressBatchResponse response = new Ma031SetDefaultAddressBatchResponse();
            if (StringUtil.isNotEmpty(v_acct_per_row.getAccountId())) {
                //调用MA031_SET_DEFAULT_ADDRESS
                response = ma031SetDefaultAddress(request1);
            } else {
                //调用GC010_GET_ADDRESS_FROM_PER
                response = gc010GetAddressFromPer(request1);
            }
            v_bill_extr_address1 = response.getPoAddressLine1();
            v_bill_extr_address2 = response.getPoAddressLine2();
            v_bill_extr_address3 = response.getPoAddressLine3();
            v_bill_extr_address4 = response.getPoAddressLine4();
        }
        //调用SQ510_GET_SUFFIX.ZS690_SETUP_SQL_S_SUFFIX存储过程
        CustAccountEntity accountEntity = accountMapper.selectOne(new QueryWrapper<CustAccountEntity>().lambda()
                .eq(CustAccountEntity::getAccountId, request.getPiBillRow().getAccountId()));
//        .eq(CustAccountEntity::getPersonId, request.getPiBillRoutingRow().getPersonId())
        String v_name_pfx_sfx = accountEntity.getNamePrefxSuffx();
        String v_pfx_sfx_flg = accountEntity.getPrefxSuffxInd();
        String v_chi_entity_name = "";
        if ("XY".equals(v_pfx_sfx_flg)) {
            v_name_pfx_sfx = v_name_pfx_sfx.trim();
            if (request.getPiSeqno() > 1) {
                v_bp_extr_entity_name1 = v_bp_extr_entity_name1.replace(v_name_pfx_sfx, "");
                if ("P".equals(request.getPiPerRow().getIdvBusSw().trim())) {
                    //调用PA280_CALL_CMPBNMFX存储过程
                    v_bp_extr_entity_name1 = pa280CallCmpbnmfx(request.getPiPerRow().getPersonId(), v_bp_extr_entity_name1);
                }
            } else {
                if (request.getPiSeqno() == 1 && request.getPiBillRoutingRow().getExtrDate() == null) {
                    if ("ZHT".equals(request.getPiLanguageCd()) || "CHI".equals(request.getPiLanguageCd())) {
                        //调用MA010_GET_CHI_NAME.XQ021_GET_S_CHINM.ZS110_SETUP_SQL_S_CHINM存储过程
                        PersonNameEntity personNameEntity = personNameMapper.selectOne(new QueryWrapper<PersonNameEntity>().lambda()
                                .eq(PersonNameEntity::getPersonId, request.getPiBillRoutingRow().getPersonId())
                                .eq(PersonNameEntity::getNameTypeCode, "PRIC"));
                        v_chi_entity_name = personNameEntity.getPstName();
                    }
                    if (("ZHT".equals(request.getPiLanguageCd()) || "CHI".equals(request.getPiLanguageCd())) && v_chi_entity_name != null && !v_chi_entity_name.trim().isEmpty()) {
                        // 调用 name_line_cut 方法
                        List<String> v_name_line_array = name_line_cut(v_chi_entity_name, 64, 3);
                        // 处理空值
                        v_bp_extr_entity_name1 = v_name_line_array.size() > 0 ? v_name_line_array.get(0) : " ";
                        v_bp_extr_entity_name2 = v_name_line_array.size() > 1 ? v_name_line_array.get(1) : " ";
                        v_bp_extr_entity_name3 = v_name_line_array.size() > 2 ? v_name_line_array.get(2) : " ";
                    } else {
                        String v_entity_name;
                        if ("P".equals(request.getPiPerRow().getIdvBusSw())) {
                            //调用PA280_CALL_CMPBNMFX存储过程
                            v_entity_name = pa280CallCmpbnmfx(request.getPiPerRow().getPersonId(), request.getPiEntityName());
                        } else {
                            v_entity_name = request.getPiEntityName();
                        }
                        //调用方法name_line_cut
                        List<String> v_name_line_array = name_line_cut(v_entity_name, 64, 3);
                        // 处理空值
                        v_bp_extr_entity_name1 = v_name_line_array.size() > 0 ? v_name_line_array.get(0) : " ";
                        v_bp_extr_entity_name2 = v_name_line_array.size() > 1 ? v_name_line_array.get(1) : " ";
                        v_bp_extr_entity_name3 = v_name_line_array.size() > 2 ? v_name_line_array.get(2) : " ";
                    }

                } else {
                    if ("P".equals(request.getPiPerRow().getIdvBusSw().trim())) {
                        //调用PA280_CALL_CMPBNMFX存储过程
                        v_bp_extr_entity_name1 = pa280CallCmpbnmfx(request.getPiPerRow().getPersonId(), v_bp_extr_entity_name1);
                    }
                }
            }
            // 对 v_bp_extr_entity_name2 进行去空格操作
            String trimmedName2 = v_bp_extr_entity_name2 != null ? v_bp_extr_entity_name2.trim() : null;
            // 对 v_bp_extr_entity_name3 进行去空格操作
            String trimmedName3 = v_bp_extr_entity_name3 != null ? v_bp_extr_entity_name3.trim() : null;

            if (trimmedName2 == null || trimmedName2.isEmpty()) {
                v_bp_extr_entity_name2 = v_name_pfx_sfx;
            } else {
                if (trimmedName3 == null || trimmedName3.isEmpty()) {
                    v_bp_extr_entity_name3 = v_name_pfx_sfx;
                } else {
                    // 拼接字符串并检查长度
                    String combinedName = trimmedName3 + " " + v_name_pfx_sfx;
                    if (combinedName.length() <= 64 && trimmedName3.indexOf(v_name_pfx_sfx) == -1) {
                        v_bp_extr_entity_name3 = combinedName.substring(0, Math.min(64, combinedName.length()));
                    }
                }
            }
        } else {
            if ("P".equals(request.getPiPerRow().getIdvBusSw().trim())) {
                //调用PA280_CALL_CMPBNMFX存储过程
                v_bp_extr_entity_name1 = pa280CallCmpbnmfx(request.getPiPerRow().getPersonId(), v_bp_extr_entity_name1);
            }
        }
        double v_bp_extr_tot_amt_due = 0;
        double v_bp_extr_autopay_amt = 0;
        double v_bp_extr_excess_amt = 0;

        // 第一个条件判断
        if (params.getMaxAmtDue() != null && (gvGlobalVariableBatch1.getW_tot_amt() >= params.getMaxAmtDue() ||
                (request.getPiCustClRow() != null && "Y".equals(request.getPiCustClRow().getOpenAcctFlg())) ||
                "Y".equals(request.getPiFinalBillSw()) ||
                "Y".equals(request.getPiNonconSw()))) {
            if (gvGlobalVariableBatch1.getW_tot_amt() > 0) {
                v_bp_extr_tot_amt_due = gvGlobalVariableBatch1.getW_tot_amt();
                gvGlobalVariableBatch1.setW_min_amt_due_s("N");
            } else {
                v_bp_extr_tot_amt_due = 0;
                gvGlobalVariableBatch1.setW_min_amt_due_s("Y");
            }
        } else {
            v_bp_extr_tot_amt_due = 0;
            gvGlobalVariableBatch1.setW_min_amt_due_s("Y");
        }
        v_bp_extr_tot_amt_due = gvGlobalVariableBatch1.getW_tot_amt();

        // 第二个条件判断
        if ("Y".equals(request.getPiWithAutopaySw())) {
            if (request.getPiExcessAmt() > 0) {
                v_bp_extr_autopay_amt = request.getPiAutopayAmt();
                gvGlobalVariableBatch1.setW_excess_amt(request.getPiExcessAmt());
                v_bp_extr_excess_amt = request.getPiExcessAmt();
            } else {
                if ("Y".equals(request.getPiFinalBillSw()) && request.getPiAutopayAmt() > 0) {
                    v_bp_extr_autopay_amt = request.getPiAutopayAmt();
                } else {
                    if (gvGlobalVariableBatch1.getW_tot_amt() < params.getMaxAmtDue()) {
                        v_bp_extr_autopay_amt = 0;
                    } else {
                        v_bp_extr_autopay_amt = request.getPiAutopayAmt();
                    }
                }
            }
        }
        double po_apay_amt = v_bp_extr_autopay_amt;
        ma030response.setPoApayAmt(po_apay_amt);
        if (gvGlobalVariableBatch1.getW_excess_amt() != null && gvGlobalVariableBatch1.getW_excess_amt() > 0) {
            //调用MM010_EXCESS_AMOUNT_MSG存储过程
        }
        //调用OA020_FORMAT_DATE存储过程处理日期格式
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        String v_bp_extr_bill_dt = sdf.format(request.getPiBillRow().getBillDt());
        String v_bp_extr_due_dt = sdf.format(request.getPiBillRow().getDueDt());
        /* Account Check Digit */
        String v_acct_chk_digit = VerifyUtils.getCheckDigit(request.getPiAcctRow().getAccountId());
        v_bp_extr_acct_id = request.getPiAcctRow().getAccountId() + v_acct_chk_digit;
        String v_bp_extr_wsd_rep = "";
        if (request.getPiCustClRow() != null && "Y".equals(request.getPiCustClRow().getOpenAcctFlg())) {
            //调用函数SQ050_GET_WSDREPO.ZS260_SETUP_SQL_S_WSDREPO存储过程
            v_bp_extr_wsd_rep = zs260SetupSqlSWsdrepo(request.getPiBillRow().getBillId(), params.getStatuAttrTypeCd(), params.getWsdrepAttrTypeCd(), request.getPiLanguageCd());
        } else {
            //调用SQ160_GET_WSDREP.ZS250_SETUP_SQL_S_WSDREP存储过程
            v_bp_extr_wsd_rep = zs250SetupSqlSWsdrep(params.getWsdrepAttrTypeCd(), request.getPiLanguageCd());
        }
        if (v_bp_extr_wsd_rep != null && v_bp_extr_wsd_rep.length() > 16) {
            //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
            zz000SetupSqlICiMsg(request.getPiBillRow().getBillId(), "30", "v_bp_extr_wsd_rep len>16:" + v_bp_extr_wsd_rep);
        }
        /* Bill Type Info */
        String v_bp_extr_bill_type_info_sw = "";
        if ("Y".equals(request.getPiFinalBillSw())) {
            if (request.getPiBillRoutingRow().getSeq() != 1) {
                v_bp_extr_bill_type_info_sw = "06"; // BILL-TYPE-DUPLICATE-FINAL-BILL
            } else {
                v_bp_extr_bill_type_info_sw = "02"; // BILL-TYPE-FINAL-BILL
            }
        } else {
            if (request.getPiBillRoutingRow().getSeq() != 1) {
                v_bp_extr_bill_type_info_sw = "05"; // BILL-TYPE-DUPLICATE-DFP
            } else {
                v_bp_extr_bill_type_info_sw = "01"; // BILL-TYPE-DEMAND-FOR-PAY
            }
        }
        /* Bill Type */
        String v_bp_extr_bill_type = "";
        if (request.getPiCustClRow() != null && "Y".equals(request.getPiCustClRow().getOpenAcctFlg())) {
            v_bp_extr_bill_type = "02"; // OPEN-ITEM
        } else {
            v_bp_extr_bill_type = "01"; // BALANCE-FORWARD
        }
        /* Charge No. */
        String v_bp_extr_charge_no = "";
        if ("01".equals(v_bp_extr_bill_type)) {
            v_bp_extr_charge_no = request.getPiAcctRow().getAccountId() + v_acct_chk_digit;
        } else {
            v_bp_extr_charge_no = ProcedureUtil.number12To13(request.getPiBillRow().getBillId());
        }
        gvBillExtractionHeaderBatch1.setSortKeyInsertCd("N");
        if ("ZHT".equals(request.getPiLanguageCd()) || "CHI".equals(request.getPiLanguageCd())) {
            gvBillExtractionHeaderBatch1.setSortKeyLanguageCd("2");
        } else {
            gvBillExtractionHeaderBatch1.setSortKeyLanguageCd("1");
        }
        gvBillExtractionHeaderBatch1.setSortKeyPerId(request.getPiMainPerId());
        gvBillExtractionHeaderBatch1.setSortKeyBillSortType(gv_bill_sort_type);
        gvBillExtractionHeaderBatch1.setSortKeyBillId(request.getPiBillRoutingRow().getBillId());
        gvBillExtractionHeaderBatch1.setSortKeySeqno(request.getPiBillRoutingRow().getSeq());
        gvBillExtractionHeaderBatch1.setSortKeyCopyNbr(1);
        gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("10");
        /* Prepare the Mailing keys for the bill */
        gvBillExtractionHeaderBatch1.setMailingKeyPostal(" ");
        //调用XQ034_CHECK_OVERDUE_MSG.ZS990_SETUP_SQL_S_OD_MSG存储过程
        Integer v_od_msg_cnt = zs990SetupSqlSOdMsg(request.getPiBillRoutingRow().getBillId(), "CM_OVERDUE_MSG");
        String v_od_prm_amt = "";
        String v_od_prm_date = "";
        if (v_od_msg_cnt > 0) {
            gvBillExtractionHeaderBatch1.setMailingKeyOverdueSw("S");
            if ("N".equals(request.getPiSkipBillSw())) {
                //调用XQ036_GET_OVERDUE_PARM.ZZ992_SETUP_SQL_S_BILL_PRM存储过程
                //BILL_MSG_PARAM表中SEQ = 1，2的 MSG_PARAM_VAL的信息BILL_MSG_PARAM表删除，pi_bill_msg_cd = 'OD33'
                BillMsgParamEntity od331 = billMsgParamMapper.selectOne(Wrappers.<BillMsgParamEntity>query().lambda()
                        .eq(BillMsgParamEntity::getBillId, request.getPiBillRow().getBillId())
                        .eq(BillMsgParamEntity::getSeq, 1)
                        .eq(BillMsgParamEntity::getBillMsgCode, "OD33"));
                //调用XQ036_GET_OVERDUE_PARM.ZZ992_SETUP_SQL_S_BILL_PRM存储过程
                BillMsgParamEntity od332 = billMsgParamMapper.selectOne(Wrappers.<BillMsgParamEntity>query().lambda()
                        .eq(BillMsgParamEntity::getBillId, request.getPiBillRow().getBillId())
                        .eq(BillMsgParamEntity::getSeq, 2)
                        .eq(BillMsgParamEntity::getBillMsgCode, "OD33"));
                if (od331 != null) {
                    v_od_prm_amt = od331.getMsgParamVal();
                    v_od_prm_date = od332.getMsgParamVal();
                }
            }
        } else {
            gvBillExtractionHeaderBatch1.setMailingKeyOverdueSw(" ");
        }
        ma030response.setPoAmtDue(v_bp_extr_tot_amt_due);
        String v_bp_extr_dtl = "";
        if ("SX".equals(v_pfx_sfx_flg) && request.getPiSeqno() == 1) {
            BillExtrApiCipbblrrBatchRequest request2 = new BillExtrApiCipbblrrBatchRequest();
            request2.setPiBillId(request.getPiBillRoutingRow().getBillId());
            request2.setPiSeqno(request.getPiBillRoutingRow().getSeq());
            request2.setPiRowAction("H");
            request2.setPiEntityName1(v_bp_extr_entity_name1);
            request2.setPiEntityName2(v_bp_extr_entity_name2);
            request2.setPiEntityName3(v_bp_extr_entity_name3);
            //调用PA020_BILL_RTG_ROW_MAINT存储过程
            billExtrApiConterPkgService.billExtrApiCipbblrr(request2);
        }
        //调用ct_detail_for_printer_key_0100存储过程
        v_bp_extr_dtl = ctDetailForPrinterKey0100(
                v_bp_extr_bill_dt,
                v_bp_extr_bill_type_info_sw,
                1,
                v_bp_extr_acct_id,
                v_bp_extr_bill_type,
                v_bp_extr_charge_no,
                v_currency_symbol,
                BigDecimal.valueOf(v_bp_extr_tot_amt_due).setScale(2, RoundingMode.HALF_UP),
                BigDecimal.valueOf(v_bp_extr_autopay_amt).setScale(2, RoundingMode.HALF_UP),
                BigDecimal.valueOf(v_bp_extr_excess_amt).setScale(2, RoundingMode.HALF_UP),
                v_bp_extr_due_dt,
                v_bp_extr_entity_name1,
                v_bp_extr_entity_name2,
                v_bp_extr_entity_name3,
                v_bill_extr_address1,
                v_bill_extr_address2,
                v_bill_extr_address3,
                v_bill_extr_address4,
                v_bp_extr_wsd_rep
        );
        gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(" ");
        gvBillExtractionHeaderBatch1.setSortKeyPreId("0");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp(" ");
        gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
        gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
        gvBillExtractionHeaderBatch1.setSortKeySdGrp(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemId(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(0);
        gvBillExtractionHeaderBatch1.setSortKeyBitemGrp(" ");
        gvBillExtractionHeaderBatch1.setSortKeyLineSeq(" ");
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(request.getPiBillRow().getBillId(), v_bp_extr_lines, request.getPiUserId(), request.getPiProcessDttm());
        gvBillExtractionHeaderBatch1.setMailingKeyOverdueSw(" ");
        ma030response.setPoBillType(v_bp_extr_bill_type);
        ma030response.setPoAcctChkDigit(v_acct_chk_digit);
        ma030response.setPoOdPrmAmt(v_od_prm_amt);
        ma030response.setPoOdPrmDate(v_od_prm_date);
        return ma030response;
    }

    public static String ctDetailForPrinterKey0100(
            String pi_bill_dt,
            String pi_bill_type_info_sw,
            int pi_nbr_bill_copies,
            String pi_acct_id,
            String pi_bill_type,
            String pi_charge_no,
            String pi_acct_cur_symbol,
            BigDecimal pi_total_amt_due,
            BigDecimal pi_autopay_amt,
            BigDecimal pi_excess_amt,
            String pi_due_dt,
            String pi_entity_name1,
            String pi_entity_name2,
            String pi_entity_name3,
            String pi_address1,
            String pi_address2,
            String pi_address3,
            String pi_address4,
            String pi_wsd_rep
    ) {
        pi_address1 = pi_address1 == null ? "" : pi_address1;
        pi_address2 = pi_address2 == null ? "" : pi_address2;
        pi_address3 = pi_address3 == null ? "" : pi_address3;
        pi_address4 = pi_address4 == null ? "" : pi_address4;
        StringBuilder v_output = new StringBuilder();

        // 填充 pi_bill_dt，右对齐，不足 10 位用空格补齐
        v_output.append(String.format("%-10s", pi_bill_dt));

        // 填充 pi_bill_type_info_sw，右对齐，不足 2 位用空格补齐
        v_output.append(String.format("%-2s", pi_bill_type_info_sw));

        // 填充 pi_nbr_bill_copies，左对齐，不足 1 位用 0 补齐
        v_output.append(String.format("%01d", pi_nbr_bill_copies));

        // 填充 pi_acct_id，右对齐，不足 11 位用空格补齐
        v_output.append(String.format("%-11s", pi_acct_id));

        // 填充 pi_bill_type，右对齐，不足 2 位用空格补齐
        v_output.append(String.format("%-2s", pi_bill_type));

        // 填充 pi_charge_no，右对齐，不足 13 位用空格补齐
        v_output.append(String.format("%-13s", pi_charge_no));

        // 填充 pi_acct_cur_symbol，右对齐，不足 4 位用空格补齐
        v_output.append(String.format("%-4s", pi_acct_cur_symbol));

        // 处理 pi_total_amt_due
        String totalAmtSign = pi_total_amt_due.compareTo(BigDecimal.ZERO) < 0 ? "-" : "+";
        long totalAmtAbs = Math.abs(pi_total_amt_due.multiply(BigDecimal.valueOf(100)).longValue());
        v_output.append(totalAmtSign).append(String.format("%015d", totalAmtAbs));

        // 处理 pi_autopay_amt
        String autopayAmtSign = pi_autopay_amt.compareTo(BigDecimal.ZERO) < 0 ? "-" : "+";
        long autopayAmtAbs = Math.abs(pi_autopay_amt.multiply(BigDecimal.valueOf(100)).longValue());
        v_output.append(autopayAmtSign).append(String.format("%015d", autopayAmtAbs));

        // 处理 pi_excess_amt
        String excessAmtSign = pi_excess_amt.compareTo(BigDecimal.ZERO) < 0 ? "-" : "+";
        long excessAmtAbs = Math.abs(pi_excess_amt.multiply(BigDecimal.valueOf(100)).longValue());
        v_output.append(excessAmtSign).append(String.format("%015d", excessAmtAbs));

        // 填充 pi_due_dt，右对齐，不足 10 位用空格补齐
        v_output.append(String.format("%-10s", pi_due_dt));

        // 处理 pi_entity_name1
        v_output.append(String.format("%-64s", pi_entity_name1), 0, 64);

        // 处理 pi_entity_name2
        v_output.append(String.format("%-64s", pi_entity_name2), 0, 64);

        // 处理 pi_entity_name3
        v_output.append(String.format("%-64s", pi_entity_name3), 0, 64);

        // 处理 pi_address1
        v_output.append(String.format("%-64s", pi_address1), 0, 64);

        // 处理 pi_address2
        v_output.append(String.format("%-64s", pi_address2), 0, 64);

        // 处理 pi_address3
        v_output.append(String.format("%-64s", pi_address3), 0, 64);

        // 处理 pi_address4
        v_output.append(String.format("%-64s", pi_address4), 0, 64);

        // 处理 pi_wsd_rep
        v_output.append(String.format("%-64s", pi_wsd_rep), 0, 64);

        return v_output.toString().trim();
    }

    public String constructBillExtrLine(String pi_details) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        return constructBillExtrLine(pi_details, gvBillExtractionHeaderBatch1.getSortKeyPreId());
    }

    public String constructBillExtrLine(String pi_details, String premiseId) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        // 构造 BILL-PRINT-PRINTER-KEY
        String v_bill_print_printer_key = String.format("%-4s", gvBillExtractionHeaderBatch1.getBillPrintPrinterKey());

        // 构造 BILL-PRINT-SORT-KEY
        String v_bill_print_sort_key = String.format("%-1s", gvBillExtractionHeaderBatch1.getSortKeyLanguageCd()) +
                String.format("%-1s", gvBillExtractionHeaderBatch1.getSortKeyInsertCd()) +
                String.format("%-1s", gvBillExtractionHeaderBatch1.getSortKeyBillSortType()) +
                String.format("%-10s", gvBillExtractionHeaderBatch1.getSortKeyPerId()) +
                String.format("%-12s", gvBillExtractionHeaderBatch1.getSortKeyBillId()) +
                String.format("%02d", gvBillExtractionHeaderBatch1.getSortKeySeqno()) +
                String.format("%01d", gvBillExtractionHeaderBatch1.getSortKeyCopyNbr()) +
                String.format("%-2s", gvBillExtractionHeaderBatch1.getSortKeyBillRecGrp()) +
                String.format("%-2s", gvBillExtractionHeaderBatch1.getSortKeyPrePrintPriority() == null ? "00" :
                        gvBillExtractionHeaderBatch1.getSortKeyPrePrintPriority());
//              String.format("%010s", gvBillExtractionHeaderBatch1.getSortKeyPreId()) +
        // 处理 sort_key_pre_id 的格式化
        // 针对 sort_key_pre_id 字段，先获取其原始值，然后定义了宽度 preIdWidth 为 10。
        // 接着检查该字段的长度是否小于指定宽度，如果小于，则使用 String.format 方法进行填充，
        // 将 0 填充到字符串的前面，使其长度达到指定宽度；如果长度已经满足或超过指定宽度，则不进行填充操作。
        // 通过这种方式，避免了因字符串长度超过指定宽度而使用 0 填充标志导致的异常问题。
        String preId = premiseId;
        if (preId == null) {
            preId = "0000000001";
        }
        int preIdWidth = 10;
        if (preId.length() < preIdWidth) {
//            preId = String.format("%0" + preIdWidth + "s", preId);
            preId = String.format("%1$" + preIdWidth + "s", preId).replace(' ', '0');
        }
        v_bill_print_sort_key += preId;
        v_bill_print_sort_key +=
                String.format("%-2s", gvBillExtractionHeaderBatch1.getSortKeyPreRecGrp()) +
                        String.format("%-2s", gvBillExtractionHeaderBatch1.getSortKeySdPrintPriority()) +
                        String.format("%-10s", gvBillExtractionHeaderBatch1.getSortKeySvcId()) +
                        String.format("%-2s", gvBillExtractionHeaderBatch1.getSortKeySdGrp()) +
                        String.format("%-10s", gvBillExtractionHeaderBatch1.getSortKeyBitemEndDate()) +
                        String.format("%-12s", gvBillExtractionHeaderBatch1.getSortKeyBitemId()) +
                        String.format("%02d", gvBillExtractionHeaderBatch1.getSortKeyBitemCalchdrSeq()) +
                        String.format("%-2s", gvBillExtractionHeaderBatch1.getSortKeyBitemGrp()) +
                        String.format("%-4s", gvBillExtractionHeaderBatch1.getSortKeyLineSeq());

        // 构造 BILL-PRINT-MAILING-KEY
        String v_bill_print_mailing_key = String.format("%-12s", gvBillExtractionHeaderBatch1.getMailingKeyPostal()) +
                String.format("%-1s", gvBillExtractionHeaderBatch1.getMailingKeyOverdueSw()) +
                "  ";
        // 拼接最终结果
        return v_bill_print_printer_key +
                v_bill_print_sort_key +
                v_bill_print_mailing_key +
                pi_details;
    }

    @Override
    public List<String> name_line_cut(String pi_input_string, int pi_max_len, int pi_cut_num) {
        List<String> po_name_line_array = new ArrayList<>();
        String tmpName = pi_input_string != null ? pi_input_string.trim() : null;
        if (tmpName == null || tmpName.isEmpty()) {
            return null;
        }
        // 扩展数组
        for (int i = 0; i < pi_cut_num; i++) {
            po_name_line_array.add(null);
        }
        for (int idx1 = 0; idx1 < pi_cut_num; idx1++) {
            tmpName = tmpName + " ";
            int tmpPos = -1;
            // 找到最后一个空格的位置
            if (tmpName.length() > pi_max_len + 1) {
                tmpPos = tmpName.substring(0, pi_max_len + 1).lastIndexOf(' ');
            } else {
                tmpPos = tmpName.lastIndexOf(' ');
            }
            String currentPart;
            String remainName;
            if (tmpPos > 0) {
                currentPart = tmpName.substring(0, tmpPos).trim();
                remainName = tmpName.substring(tmpPos + 1).trim();
            } else {
                currentPart = tmpName.substring(0, Math.min(tmpName.length() - 1, pi_max_len)).trim();
                remainName = tmpName.substring(Math.min(tmpName.length() - 1, pi_max_len)).trim();
            }
            po_name_line_array.set(idx1, currentPart);
            tmpName = remainName;
        }
        return po_name_line_array;
    }

    @Override
    public String zs260SetupSqlSWsdrepo(String piBillId, String piStatuAttrTypeCd, String piWsdrepAttrTypeCd, String piLanguageCd) {
        return billExtrMapper.zs260SetupSqlSWsdrepo(piBillId, piStatuAttrTypeCd, piWsdrepAttrTypeCd, piLanguageCd);
    }

    @Override
    public String zs250SetupSqlSWsdrep(String piWsdrepAttrTypeCd, String piLanguageCd) {
        return billExtrMapper.zs250SetupSqlSWsdrep(piWsdrepAttrTypeCd, piLanguageCd);
    }

    @Override
    public Integer zs990SetupSqlSOdMsg(String piBillId, String piFieldName) {
        return billExtrMapper.zs990SetupSqlSOdMsg(piBillId, piFieldName);
    }

    public void ma040PaymentRec(Ma040PaymentRecBatchRequest request,
                                List<FinTranWithAdj> finTranWithAdjList,
                                List<BitemWithReadAndQty> bitemWithReadAndQtyList, String gv_bill_sort_type) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        /* Payments */
        //调用SQ260_GET_LASTAPAY.ZS320_SETUP_SQL_S_LASTAPAY存储过程
        Map<String, Object> dcad = zs320SetupSqlSLastapay(request.getPiBillRow().getAccountId(), "DCAD", request.getPiBillRow().getCmpltDt());
        BigDecimal v_bbpxt_d_payment_last_pay_amt = (BigDecimal) dcad.get("payment_amount_sum");
        String v_bbpxt_d_payment_last_pay_dt = (String) dcad.get("max_payment_date");
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("0210");
        gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("20");
        /* Get Deposit Paid */
        //调用SQ280_GET_DPPAID.ZS370_SETUP_SQL_S_DPPAID存储过程
        BigDecimal v_bbpxt_d_payment_depost_paid = zs370SetupSqlSDppaid(request.getPiBillRow().getAccountId(), request.getPiBillRow().getCmpltDt());
        /* Get Dispute Amount */
        //调用SQ290_GET_DISPAMT.ZS380_SETUP_SQL_S_DISPAMT存储过程
        BigDecimal v_bbpxt_d_payment_dispute_amt = zs380SetupSqlSDispamt(request.getPiBillRow().getBillId(), params.getDisputeFldName());
        BigDecimal v_bbpxt_d_payment_inst_amt = request.getPiInstamtInstAmt();
        String v_bp_extr_dtl;
        String v_bp_extr_lines;
        //调用ct_detail_for_printer_key_0210函数
        v_bp_extr_dtl = ctDetailForPrinterKey0210(v_bbpxt_d_payment_last_pay_dt, v_bbpxt_d_payment_last_pay_amt, v_bbpxt_d_payment_depost_paid, v_bbpxt_d_payment_dispute_amt, v_bbpxt_d_payment_inst_amt);
        //调用construct_bill_extr_line函数
        v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(request.getPiBillRow().getBillId(), v_bp_extr_lines, request.getPiUserId(), request.getPiProcessDttm());

        /* Payment Slip Record */
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("5000");
        gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("70");
        /* Date of Issue */
        //调用OA020_FORMAT_DATE转换日期格式
        SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
        String v_bbpxt_payslp_dt_of_issue = sdf2.format(request.getPiBillRow().getBillDt());
        /* Charge No */
        String v_bbpxt_payslp_charge_no = "";
        if ("01".equals(request.getPiBillType())) {
            v_bbpxt_payslp_charge_no = request.getPiBillRow().getAccountId() +
                    request.getPiAcctIdWithCheckDigit();
        } else {
            v_bbpxt_payslp_charge_no = ProcedureUtil.number12To13(request.getPiBillRow().getBillId());
        }
        /* Amount Due */
        //调用FNC_PAYABLE_AMOUNT_SPECIAL_HANDLE函数
        BigDecimal v_bbpxt_payslp_amount_due = fncPayableAmountSpecialHandle(request.getPiBillRow().getBillId(), BigDecimal.valueOf(gvGlobalVariableBatch1.getW_tot_amt()),
                BigDecimal.valueOf(params.getMaxAmtDue() == null ? 0.0 : params.getMaxAmtDue()), params.getFinalBillMsgCd(), params.getInstFldName(), params.getDisputeFldName());
        int v_bbpxt_payslp_serial_no = 0;
        if ("Y".equals(gvGlobalVariableBatch1.getGv_batch_mode())) {
            v_bbpxt_payslp_serial_no = request.getPiBillCnt() + 1;
        } else {
            v_bbpxt_payslp_serial_no = 1;
        }
        String v_surg_not_found_sw = "N";
        //调用SQ400_GET_SURCHG.ZS490_SETUP_SQL_S_SURCHG存储过程
        Zs490SetupSqlSSurchgBatchResponse zs490SetupSqlSSurchgResponse = zs490SetupSqlSSurchg(request.getPiBillRow().getBillId(),
                params.getSurchgBitemCd(), v_surg_not_found_sw);
        String v_bbpxt_payslp_5_surcharge_dt = "";
        String v_bbpxt_payslp_5_surcharge = "";
        String v_bbpxt_payslp_10_surcharge_dt = "";
        String v_bbpxt_payslp_10_surcharge = "";
        Double v_tot_amt_due = 0.0;
        if ("N".equals(zs490SetupSqlSSurchgResponse.getPoSurgNotFoundSw())) {
            v_bbpxt_payslp_5_surcharge_dt = zs490SetupSqlSSurchgResponse.getPoSur5Dt();
            v_bbpxt_payslp_5_surcharge = zs490SetupSqlSSurchgResponse.getPoSur5Amt();
            v_bbpxt_payslp_10_surcharge_dt = zs490SetupSqlSSurchgResponse.getPoSur10Dt();
            v_bbpxt_payslp_10_surcharge = zs490SetupSqlSSurchgResponse.getPoSur10Amt();
        } else {
            if ("Y".equals(gvGlobalVariableBatch1.getW_min_amt_due_s())) {
                v_tot_amt_due = 0.0;
            } else {
                v_tot_amt_due = gvGlobalVariableBatch1.getW_tot_amt();
            }
            //调用PA270_SURCHARGE_MAINT存储过程 --- 没有返回值 省略
            Map<String,Object> surchgData = pa270SurchargeMaint(request.getPiProcessDttm(), "B", request.getPiBillRow().getBillId(),
                    " ", v_tot_amt_due, params.getSurchgBitemCd(),
                    "TIER2LPC", finTranWithAdjList, bitemWithReadAndQtyList);
            //调用SQ400_GET_SURCHG.ZS490_SETUP_SQL_S_SURCHG存储过程
            zs490SetupSqlSSurchgResponse = zs490SetupSqlSSurchg(request.getPiBillRow().getBillId(),
                    params.getSurchgBitemCd(), v_surg_not_found_sw);
            v_bbpxt_payslp_5_surcharge_dt = zs490SetupSqlSSurchgResponse.getPoSur5Dt();
            v_bbpxt_payslp_5_surcharge = zs490SetupSqlSSurchgResponse.getPoSur5Amt();
            v_bbpxt_payslp_10_surcharge_dt = zs490SetupSqlSSurchgResponse.getPoSur10Dt();
            v_bbpxt_payslp_10_surcharge = zs490SetupSqlSSurchgResponse.getPoSur10Amt();
        }
        //调用SQ540_GET_CRCV.ZS650_SETUP_SQL_S_CRCV存储过程
        String v_bbpxt_payslp_crc_no = zs650SetupSqlSCrcv(params.getCrcvAttrTypeCd());
        String v_bbpxt_payslp_sklip_type = "";
        if ("2".equals(gv_bill_sort_type)) {
            v_bbpxt_payslp_sklip_type = "M";// MONTHLY
        } else {
            v_bbpxt_payslp_sklip_type = " ";// OTHERS
        }
        BigDecimal surc5 = BigDecimal.ZERO;
        BigDecimal surc10 = BigDecimal.ZERO;
        if (v_bbpxt_payslp_5_surcharge != null) {
            surc5 = new BigDecimal(v_bbpxt_payslp_5_surcharge.trim());
        }
        if (v_bbpxt_payslp_10_surcharge != null) {
            surc10 = new BigDecimal(v_bbpxt_payslp_10_surcharge.trim());
        }
        //调用ct_detail_for_printer_key_5000函数获取v_bp_extr_dtl信息
        v_bp_extr_dtl = ctDetailForPrinterKey5000(
                v_bbpxt_payslp_dt_of_issue,
                v_bbpxt_payslp_charge_no,
                v_bbpxt_payslp_amount_due,
                v_bbpxt_payslp_5_surcharge_dt,
                surc5,
                v_bbpxt_payslp_10_surcharge_dt,
                surc10,
                v_bbpxt_payslp_crc_no,
                v_bbpxt_payslp_serial_no,
                v_bbpxt_payslp_sklip_type
        );
        //调用construct_bill_extr_line函数
        v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(request.getPiBillRow().getBillId(), v_bp_extr_lines, request.getPiUserId(), request.getPiProcessDttm());
    }

    /**
     * billExtrMapper.zs320SetupSqlSLastapay(piAcctId, piPaymentTypeCd, piCompleteDttm);
     */
    @Override
    public Map<String, Object> zs320SetupSqlSLastapay(String piAcctId, String piPaymentTypeCd, Date piCompleteDttm) {
        Map<String, Object> retVal = new HashMap<>();
        BigDecimal lastPaymentAmount = BigDecimal.ZERO;
        Date lastPaymentDate = null;
        List<PaymentWithDtl> paymentWithDtlList = billExtrMapper.selectPaymentDtl(piAcctId, piCompleteDttm);
        if (paymentWithDtlList.isEmpty()) {
            return retVal;
        }
        Set<String> paymentDtlIds = new HashSet<>();
        for (PaymentWithDtl paymentWithDtl : paymentWithDtlList) {
            if (paymentWithDtl.getPaymentDate().after(piCompleteDttm)) {
                continue;
            }
            paymentDtlIds.add(paymentWithDtl.getPaymentDtlId());
        }
        List<FinTranDto> finTranDtoList = null;
        if (paymentDtlIds.isEmpty()) {
            finTranDtoList = new ArrayList<>();
        } else {
            finTranDtoList = billExtrMapper.selectFinTranByPaymentDtlIds(paymentDtlIds);
        }
        Set<String> matchingPaymentDtlIds = new HashSet<>();
        for (PaymentWithDtl row : paymentWithDtlList) {
            if (row.getPaymentDate().after(piCompleteDttm)) {
                continue;
            }
            if (row.getPaymentDtlAmt().compareTo(BigDecimal.ZERO) == 0 ||
                piPaymentTypeCd.equals(row.getPaymentTypeCode())) {
                continue;
            }
            boolean orLeftSide = false;
            if ("50".equals(row.getPaymentDtlStsInd())) {
                if (!existFinTranWithPayment(row, finTranDtoList, "PS")) {
                    orLeftSide = true;
                }
            }
            boolean orRightSide = false;
            if ("50".equals(row.getPaymentDtlStsInd()) || "60".equals(row.getPaymentDtlStsInd())) {
                if (existFinTranWithPayment(row, finTranDtoList, "PS") &&
                    !existFinTranWithPayment(row, finTranDtoList, "PX")) {
                    orRightSide = true;
                }
            }
            if (orLeftSide || orRightSide) {
                if (lastPaymentDate == null) {
                    lastPaymentDate = row.getPaymentDate();
                }
                if (lastPaymentDate.before(row.getPaymentDate())) {
                    lastPaymentDate = row.getPaymentDate();
                }
                matchingPaymentDtlIds.add(row.getPaymentDtlId());
            }
        }
        for (PaymentWithDtl paymentWithDtl : paymentWithDtlList) {
            if (matchingPaymentDtlIds.contains(paymentWithDtl.getPaymentDtlId()) && paymentWithDtl.getPaymentDate().equals(lastPaymentDate)) {
                lastPaymentAmount = lastPaymentAmount.add(paymentWithDtl.getPaymentDtlAmt());
            }
        }
        // payment_amount_sum
        // max_payment_date
        if (lastPaymentDate != null) {
            retVal.put("max_payment_date", DateUtils.formatDateToString(lastPaymentDate, "dd/MM/yyyy"));
        }

        retVal.put("payment_amount_sum", lastPaymentAmount);
        return retVal;
//        return billExtrMapper.zs320SetupSqlSLastapay(piAcctId, piPaymentTypeCd, piCompleteDttm);
    }

    @Override
    public BigDecimal zs370SetupSqlSDppaid(String piAcctId, Date piCompleteDttm) {
        return billExtrMapper.zs370SetupSqlSDppaid(piAcctId, piCompleteDttm);
    }

    @Override
    public Ma031SetDefaultAddressBatchResponse ma031SetDefaultAddress(Ma031SetDefaultAddressBatchRequest request) {
        Ma031SetDefaultAddressBatchResponse response = new Ma031SetDefaultAddressBatchResponse();
        String pi_bill_addr_srce_flg = request.getPiBillAddrSrceFlg();
        if ("PER".equals(pi_bill_addr_srce_flg)) {
            //调用GC010_GET_ADDRESS_FROM_PER
            response = gc010GetAddressFromPer(request);
        } else if ("PREM".equals(pi_bill_addr_srce_flg)) {
            //调用GC020_GET_ADDRESS_FROM_ACCT 存储过程
            response = gc020GetAddressFromAcct(request);
        } else if ("ACOV".equals(pi_bill_addr_srce_flg)) {
            //调用GC030_GET_OVERRIDE_ADDRESS.SQ801_GET_OVRDADDR.ZS920_SETUP_SQL_S_OVRDADDR 存储过程
            response = zs920SetupSqlSOvrdaddr(request.getPiAcctId(), request.getPiPerId());
        }
        return response;
    }

    @Override
    public Ma031SetDefaultAddressBatchResponse gc010GetAddressFromPer(Ma031SetDefaultAddressBatchRequest request) {
        Ma031SetDefaultAddressBatchResponse response = new Ma031SetDefaultAddressBatchResponse();
        //调用SQ800_GET_ACCTCH.ZS910_SETUP_SQL_S_ACCTCH 存储过程
        String v_acct_lang = zs910SetupSqlSAcctch(request.getPiAcctId(), "ACCTLANG", request.getPiBillDt());
        String v_language_cd = "";
        if (v_acct_lang != null) {
            v_language_cd = v_acct_lang;
        } else {
            v_language_cd = request.getPiPerAcctLang();
        }
        //调用PA300_CALL_CMPCGMAN.bill_extr_api_get_mailing_address_pkg.AA000_MAIN.GA000_PERFORM_PROC存储过程
        Ga000PerformProcBatchResponse res = ga000PerformProc(request.getPiAcctId(), "", v_language_cd, request.getPiProcessDttm());
        //调用GC100_MOVE_ADDRESS截取地址信息
        String poAddressLine1 = res.getPoAddressLine1();
        String poAddressLine2 = res.getPoAddressLine2();
        String poAddressLine3 = res.getPoAddressLine3();
        String poAddressLine4 = res.getPoAddressLine4();
        response.setPoAddressLine1(poAddressLine1.substring(0, 64));
        response.setPoAddressLine2(poAddressLine2.substring(0, 64));
        response.setPoAddressLine3(poAddressLine3.substring(0, 64));
        response.setPoAddressLine4(poAddressLine4.substring(0, 64));
        return response;
    }

    @Override
    public String zs910SetupSqlSAcctch(String piAcctId, String piAttrType, Date piBusDate) {

        return billExtrMapper.zs910SetupSqlSAcctch(piAcctId, piAttrType, piBusDate);
    }

    @Override
    public Ga000PerformProcBatchResponse ga000PerformProc(String piPerId, String piPremId, String piLanguageCd, Date piProcessDttm) {
        Ga000PerformProcBatchResponse response = new Ga000PerformProcBatchResponse();
        if (piPerId != null) {
            //调用IA010_GET_ADDR_FOR_PER 存储过程
            response = ia010GetAddrForPer(piPerId, piLanguageCd, piProcessDttm);
        } else if (piPremId != null) {
            //调用IA020_GET_ADDR_FOR_PREM 存储过程
            response = ia020GetAddrForPrem(piPremId, piLanguageCd);
        }
        return response;
    }

    @Override
    public Ga000PerformProcBatchResponse ia010GetAddrForPer(String piPerId, String piLanguageCd, Date piProcessDttm) {
        Ga000PerformProcBatchResponse response = new Ga000PerformProcBatchResponse();
        String v_mailaddr_char_type_cd = "MAILADDL";
        //调用XQ010_GET_S_PCHAR.ZS100_SETUP_SQL_S_PCHAR 存储过程
        String v_char_val_fk1 = zs100SetupSqlSPchar(piPerId, v_mailaddr_char_type_cd, piProcessDttm);
        if (StringUtil.isNotEmpty(v_char_val_fk1)) {
            if ("ENG".equals(piLanguageCd)) {
                //调用XQ020_GET_S_ENGADDR.ZS110_SETUP_SQL_S_ENGADDR 存储过程
                GeoAddressntity geoAddressntity = premiseMapper.selectById(v_char_val_fk1);
                String v_engaddr_line1 = "";
                String v_engaddr_line2 = "";
                String v_engaddr_line3 = "";
                String v_engaddr_line4 = "";
                if (geoAddressntity != null) {
                    v_engaddr_line1 = geoAddressntity.getAddrLn1();
                    v_engaddr_line2 = geoAddressntity.getAddrLn2();
                    v_engaddr_line3 = geoAddressntity.getAddrLn3();
                    v_engaddr_line4 = geoAddressntity.getAddrLn4();
                }

                String v_engaddr_line5 = null;
                String v_engaddr_postcode = null;
                if (StringUtil.isEmpty(v_engaddr_line1) &&
                        StringUtil.isEmpty(v_engaddr_line2) &&
                        StringUtil.isEmpty(v_engaddr_line3) &&
                        StringUtil.isEmpty(v_engaddr_line4) &&
                        StringUtil.isEmpty(v_engaddr_line5)) {
                    //调用XQ030_GET_S_CHIADDR.ZS120_SETUP_SQL_S_CHIADDR 存储过程
                    PremiseZhtEntity premiseZhtEntity = premiseZhtMapper.selectById(v_char_val_fk1);
                    String v_zhtaddr_line1 = "";
                    String v_zhtaddr_line2 = "";
                    String v_zhtaddr_line3 = "";
                    String v_zhtaddr_line4 = "";
                    if (premiseZhtEntity != null) {
                        v_zhtaddr_line1 = premiseZhtEntity.getAddrLn1();
                        v_zhtaddr_line2 = premiseZhtEntity.getAddrLn2();
                        v_zhtaddr_line3 = premiseZhtEntity.getAddrLn3();
                        v_zhtaddr_line4 = premiseZhtEntity.getAddrLn4();
                    }
                    String v_zhtaddr_line5 = null;
                    String v_zhtaddr_postcode = null;
                    response.setPoAddressLine1(v_zhtaddr_line1);
                    response.setPoAddressLine2(v_zhtaddr_line2);
                    response.setPoAddressLine3(v_zhtaddr_line3);
                    response.setPoAddressLine4(v_zhtaddr_line4);
                    response.setPoAddressLine5(v_zhtaddr_line5);
                    response.setPoAddressPostcode(v_zhtaddr_postcode);
                    response.setPoPremId(v_char_val_fk1);
                } else {
                    response.setPoAddressLine1(v_engaddr_line1);
                    response.setPoAddressLine2(v_engaddr_line2);
                    response.setPoAddressLine3(v_engaddr_line3);
                    response.setPoAddressLine4(v_engaddr_line4);
                    response.setPoAddressLine5(v_engaddr_line5);
                    response.setPoAddressPostcode(v_engaddr_postcode);
                    response.setPoPremId(v_char_val_fk1);
                }
            } else if ("ZHT".equals(piLanguageCd)) {
                //调用XQ030_GET_S_CHIADDR.ZS120_SETUP_SQL_S_CHIADDR 存储过程
                PremiseZhtEntity premiseZhtEntity = premiseZhtMapper.selectById(v_char_val_fk1);
                if (premiseZhtEntity == null) {
                    premiseZhtEntity = new PremiseZhtEntity();
                    premiseZhtEntity.setAddrLn1("");
                    premiseZhtEntity.setAddrLn2("");
                    premiseZhtEntity.setAddrLn3("");
                    premiseZhtEntity.setAddrLn4("");
                }
                String v_zhtaddr_line1 = premiseZhtEntity.getAddrLn1();
                String v_zhtaddr_line2 = premiseZhtEntity.getAddrLn2();
                String v_zhtaddr_line3 = premiseZhtEntity.getAddrLn3();
                String v_zhtaddr_line4 = premiseZhtEntity.getAddrLn4();
                String v_zhtaddr_line5 = null;
                String v_zhtaddr_postcode = null;
                if (StringUtil.isEmpty(v_zhtaddr_line1) &&
                        StringUtil.isEmpty(v_zhtaddr_line2) &&
                        StringUtil.isEmpty(v_zhtaddr_line3) &&
                        StringUtil.isEmpty(v_zhtaddr_line4) &&
                        StringUtil.isEmpty(v_zhtaddr_line5)) {
                    //调用XQ020_GET_S_ENGADDR.ZS110_SETUP_SQL_S_ENGADDR 存储过程
                    GeoAddressntity geoAddressntity = premiseMapper.selectById(v_char_val_fk1);
                    String v_engaddr_line1 = geoAddressntity.getAddrLn1();
                    String v_engaddr_line2 = geoAddressntity.getAddrLn2();
                    String v_engaddr_line3 = geoAddressntity.getAddrLn3();
                    String v_engaddr_line4 = geoAddressntity.getAddrLn4();
                    String v_engaddr_line5 = null;
                    String v_engaddr_postcode = null;
                    response.setPoAddressLine1(v_engaddr_line1);
                    response.setPoAddressLine2(v_engaddr_line2);
                    response.setPoAddressLine3(v_engaddr_line3);
                    response.setPoAddressLine4(v_engaddr_line4);
                    response.setPoAddressLine5(v_engaddr_line5);
                    response.setPoAddressPostcode(v_engaddr_postcode);
                    response.setPoPremId(v_char_val_fk1);
                } else {
                    response.setPoAddressLine1(v_zhtaddr_line1);
                    response.setPoAddressLine2(v_zhtaddr_line2);
                    response.setPoAddressLine3(v_zhtaddr_line3);
                    response.setPoAddressLine4(v_zhtaddr_line4);
                    response.setPoAddressLine5(v_zhtaddr_line5);
                    response.setPoAddressPostcode(v_zhtaddr_postcode);
                    response.setPoPremId(v_char_val_fk1);
                }
            } else {
                //调用XC010_CALL_PERSON_ROW_MAINT 存储过程
                PersonEntity personEntity = personMapper.selectById(piPerId);
                if (StringUtil.isNotEmpty(personEntity.getAddrLine1()) &&
                        StringUtil.isNotEmpty(personEntity.getAddrLine2()) &&
                        StringUtil.isNotEmpty(personEntity.getAddrLine3()) &&
                        StringUtil.isNotEmpty(personEntity.getAddrLine4())) {
                    response.setPoAddressLine1(personEntity.getAddrLine1());
                    response.setPoAddressLine2(personEntity.getAddrLine2());
                    response.setPoAddressLine3(personEntity.getAddrLine3());
                    response.setPoAddressLine4(personEntity.getAddrLine4());
                    response.setPoAddressLine5(null);
                    response.setPoAddressPostcode(" ");
                    response.setPoPremId(v_char_val_fk1);
                }
            }
        }
        return response;
    }

    @Override
    public String zs100SetupSqlSPchar(String piPerId, String piLanguageCd, Date piProcessDttm) {
        return billExtrMapper.zs100SetupSqlSPchar(piPerId, piLanguageCd, piProcessDttm);
    }

    @Override
    public Ma031SetDefaultAddressBatchResponse gc020GetAddressFromAcct(Ma031SetDefaultAddressBatchRequest request) {
        Ma031SetDefaultAddressBatchResponse response = new Ma031SetDefaultAddressBatchResponse();
        //调用PA040_ACCT_ROW_MAINT.BILL_EXTR_API_CONTER_PKG.bill_extr_api_CIPCACCR存储过程获取ACCOUNT信息
        CustAccountEntity accountEntity = accountMapper.selectById(request.getPiAcctId());
        //调用SQ800_GET_ACCTCH.ZS910_SETUP_SQL_S_ACCTCH 存储过程
        String v_acct_lang = zs910SetupSqlSAcctch(request.getPiAcctId(), "ACCTLANG", request.getPiBillDt());
        String v_language_cd = "";
        if (v_acct_lang != null) {
            v_language_cd = v_acct_lang;
        } else {
            v_language_cd = request.getPiPerAcctLang();
        }
        //调用PA300_CALL_CMPCGMAN.bill_extr_api_get_mailing_address_pkg.AA000_MAIN.GA000_PERFORM_PROC存储过程
        Ga000PerformProcBatchResponse res = ga000PerformProc(
                accountEntity.getPersonId(), accountEntity.getMailPremiseId(),
                v_language_cd, request.getPiProcessDttm());
        //调用GC100_MOVE_ADDRESS截取地址信息
        String poAddressLine1 = res.getPoAddressLine1();
        String poAddressLine2 = res.getPoAddressLine2();
        String poAddressLine3 = res.getPoAddressLine3();
        String poAddressLine4 = res.getPoAddressLine4();
        response.setPoAddressLine1(StrUtils.padRight(poAddressLine1, 64, ' '));
        response.setPoAddressLine2(StrUtils.padRight(poAddressLine2, 64, ' '));
        response.setPoAddressLine3(StrUtils.padRight(poAddressLine3, 64, ' '));
        response.setPoAddressLine4(StrUtils.padRight(poAddressLine4, 64, ' '));
        return response;
    }

    @Override
    public Ga000PerformProcBatchResponse ia020GetAddrForPrem(String piPremId, String piLanguageCd) {
        Ga000PerformProcBatchResponse response = new Ga000PerformProcBatchResponse();
        if ("ENG".equals(piLanguageCd)) {
            //调用XQ020_GET_S_ENGADDR.ZS110_SETUP_SQL_S_ENGADDR 存储过程
            GeoAddressntity geoAddressntity = premiseMapper.selectById(piPremId);
            String v_engaddr_line1 = geoAddressntity.getAddrLn1();
            String v_engaddr_line2 = geoAddressntity.getAddrLn2();
            String v_engaddr_line3 = geoAddressntity.getAddrLn3();
            String v_engaddr_line4 = geoAddressntity.getAddrLn4();
            String v_engaddr_line5 = null;
            String v_engaddr_postcode = null;
            if (StringUtil.isEmpty(v_engaddr_line1) &&
                    StringUtil.isEmpty(v_engaddr_line2) &&
                    StringUtil.isEmpty(v_engaddr_line3) &&
                    StringUtil.isEmpty(v_engaddr_line4) &&
                    StringUtil.isEmpty(v_engaddr_line5)) {
                //调用XQ030_GET_S_CHIADDR.ZS120_SETUP_SQL_S_CHIADDR 存储过程
                PremiseZhtEntity premiseZhtEntity = premiseZhtMapper.selectById(piPremId);
                if (premiseZhtEntity == null) {
                    premiseZhtEntity = new PremiseZhtEntity();
                    premiseZhtEntity.setAddrLn1("");
                    premiseZhtEntity.setAddrLn2("");
                    premiseZhtEntity.setAddrLn3("");
                    premiseZhtEntity.setAddrLn4("");
                }
                String v_zhtaddr_line1 = premiseZhtEntity.getAddrLn1();
                String v_zhtaddr_line2 = premiseZhtEntity.getAddrLn2();
                String v_zhtaddr_line3 = premiseZhtEntity.getAddrLn3();
                String v_zhtaddr_line4 = premiseZhtEntity.getAddrLn4();
                String v_zhtaddr_line5 = null;
                String v_zhtaddr_postcode = null;
                response.setPoAddressLine1(v_zhtaddr_line1);
                response.setPoAddressLine2(v_zhtaddr_line2);
                response.setPoAddressLine3(v_zhtaddr_line3);
                response.setPoAddressLine4(v_zhtaddr_line4);
                response.setPoAddressLine5(v_zhtaddr_line5);
                response.setPoAddressPostcode(v_zhtaddr_postcode);
                response.setPoPremId(piPremId);
            } else {
                response.setPoAddressLine1(v_engaddr_line1);
                response.setPoAddressLine2(v_engaddr_line2);
                response.setPoAddressLine3(v_engaddr_line3);
                response.setPoAddressLine4(v_engaddr_line4);
                response.setPoAddressLine5(v_engaddr_line5);
                response.setPoAddressPostcode(v_engaddr_postcode);
                response.setPoPremId(piPremId);
            }
        } else if ("ZHT".equals(piLanguageCd)) {
            //调用XQ030_GET_S_CHIADDR.ZS120_SETUP_SQL_S_CHIADDR 存储过程
            PremiseZhtEntity premiseZhtEntity = premiseZhtMapper.selectById(piPremId);
            if (premiseZhtEntity == null) {
                premiseZhtEntity = new PremiseZhtEntity();
                premiseZhtEntity.setAddrLn1("");
                premiseZhtEntity.setAddrLn2("");
                premiseZhtEntity.setAddrLn3("");
                premiseZhtEntity.setAddrLn4("");
            }
            String v_zhtaddr_line1 = premiseZhtEntity.getAddrLn1();
            String v_zhtaddr_line2 = premiseZhtEntity.getAddrLn2();
            String v_zhtaddr_line3 = premiseZhtEntity.getAddrLn3();
            String v_zhtaddr_line4 = premiseZhtEntity.getAddrLn4();
            String v_zhtaddr_line5 = null;
            String v_zhtaddr_postcode = null;
            if (StringUtil.isEmpty(v_zhtaddr_line1) &&
                    StringUtil.isEmpty(v_zhtaddr_line2) &&
                    StringUtil.isEmpty(v_zhtaddr_line3) &&
                    StringUtil.isEmpty(v_zhtaddr_line4) &&
                    StringUtil.isEmpty(v_zhtaddr_line5)) {
                //调用XQ020_GET_S_ENGADDR.ZS110_SETUP_SQL_S_ENGADDR 存储过程
                GeoAddressntity geoAddressntity = premiseMapper.selectById(piPremId);
                String v_engaddr_line1 = geoAddressntity.getAddrLn1();
                String v_engaddr_line2 = geoAddressntity.getAddrLn2();
                String v_engaddr_line3 = geoAddressntity.getAddrLn3();
                String v_engaddr_line4 = geoAddressntity.getAddrLn4();
                String v_engaddr_line5 = null;
                String v_engaddr_postcode = null;
                response.setPoAddressLine1(v_engaddr_line1);
                response.setPoAddressLine2(v_engaddr_line2);
                response.setPoAddressLine3(v_engaddr_line3);
                response.setPoAddressLine4(v_engaddr_line4);
                response.setPoAddressLine5(v_engaddr_line5);
                response.setPoAddressPostcode(v_engaddr_postcode);
                response.setPoPremId(piPremId);
            } else {
                response.setPoAddressLine1(v_zhtaddr_line1);
                response.setPoAddressLine2(v_zhtaddr_line2);
                response.setPoAddressLine3(v_zhtaddr_line3);
                response.setPoAddressLine4(v_zhtaddr_line4);
                response.setPoAddressLine5(v_zhtaddr_line5);
                response.setPoAddressPostcode(v_zhtaddr_postcode);
                response.setPoPremId(piPremId);
            }
        }
        return response;
    }

    @Override
    public Ma031SetDefaultAddressBatchResponse zs920SetupSqlSOvrdaddr(String piAcctId, String piPerId) {
        Ma031SetDefaultAddressBatchResponse response = new Ma031SetDefaultAddressBatchResponse();
        CustAccountEntity accountEntity = accountMapper.selectOne(new QueryWrapper<CustAccountEntity>().lambda()
                .eq(CustAccountEntity::getAccountId, piAcctId));
        response.setPoAddressLine1(accountEntity.getAddrLine1());
        response.setPoAddressLine2(accountEntity.getAddrLine2());
        response.setPoAddressLine3(accountEntity.getAddrLine3());
        response.setPoAddressLine4(accountEntity.getAddrLine4());
        return response;
    }

    @Override
    public String pa280CallCmpbnmfx(String piPersonId, String piEntityName) {
        String v_keep_original_name = "N";
        String v_surname = "";
        String v_given_name = "";
        String po_entity_name = piEntityName;
        v_keep_original_name = billExtrMapper.pa280CallCmpbnmfx(piPersonId);
        if (StringUtil.isNotEmpty(v_keep_original_name) && "Y".equals(v_keep_original_name)) {
            return po_entity_name;
        } else {
            po_entity_name = "";
            if (piEntityName.contains(",")) {
                int commaIndex = piEntityName.indexOf(",");
                v_surname = piEntityName.substring(0, commaIndex);
                v_given_name = piEntityName.substring(commaIndex + 1).trim();
            } else {
                v_surname = piEntityName;
                v_given_name = null;
            }
            v_surname = v_surname.replace("-", " ");
            po_entity_name = (v_surname + " " + (v_given_name != null ? v_given_name : "")).trim();
        }
        return po_entity_name;
    }


    private void validateBasicParams(CheckInputBatchRequest request, CheckInputBatchResponse response) {
        if (!StringUtils.hasText(request.getBillId())) {
            response.setErrorCode("253");
            response.setErrorParam("BILL_ID");
            return;
        }

        if (request.getSeqNo() == null || request.getSeqNo() <= 0) {
            response.setErrorCode("256");
            response.setErrorParam("SEQNO");
        }
    }

    private void validateAlgorithmParams(CheckInputBatchRequest request, AlgorithmParameters params,
                                         CheckInputBatchResponse response) {
        // 使用策略模式优化大量参数校验
        Map<Function<CheckInputBatchRequest, String>, String> validations = new LinkedHashMap<>();

        validations.put(CheckInputBatchRequest::getAlgParmVal1, "DEPOSIT OFFSET ADJ TYPE CD");
        validations.put(CheckInputBatchRequest::getAlgParmVal2, "DSD CHARGE TYPE CD");
        validations.put(CheckInputBatchRequest::getAlgParmVal3, "INST FIELD NAME");
        validations.put(CheckInputBatchRequest::getAlgParmVal4, "SEWAGE FIELD NAME");
        validations.put(CheckInputBatchRequest::getAlgParmVal5, "TES FIELD NAME");
        validations.put(CheckInputBatchRequest::getAlgParmVal6, "DISPUTE FIELD NAME");
        validations.put(CheckInputBatchRequest::getAlgParmVal7, "SEWAGE CHARGE RATE");
        validations.put(CheckInputBatchRequest::getAlgParmVal8, "SEWAGE CHARGE DISCHARGE FACTOR");
        validations.put(CheckInputBatchRequest::getAlgParmVal9, "TES CHARGE DISCHARGE FACTOR");
        validations.put(CheckInputBatchRequest::getAlgParmVal10, "TES RATE");
        validations.put(CheckInputBatchRequest::getAlgParmVal11, "SQI CM");
        validations.put(CheckInputBatchRequest::getAlgParmVal12, "SQI GAL");
        validations.put(CheckInputBatchRequest::getAlgParmVal13, "IGNORE PRINT SUPPRESS");
        validations.put(CheckInputBatchRequest::getAlgParmVal14, "IGNORE NEXT PRINT");
        validations.put(CheckInputBatchRequest::getAlgParmVal15, "Maximum Amount Due");
        validations.put(CheckInputBatchRequest::getAlgParmVal16, "SURCHARGE MSG CD");
        validations.put(CheckInputBatchRequest::getAlgParmVal17, "DSD DF FIELD NAME");
        validations.put(CheckInputBatchRequest::getAlgParmVal18, "CRCV Char Type Code");
        validations.put(CheckInputBatchRequest::getAlgParmVal19, "STATU Char Type Code");
        validations.put(CheckInputBatchRequest::getAlgParmVal20, "WSDREP Char Type Code");
        validations.put(CheckInputBatchRequest::getAlgParmVal21, "Cancel Bill Message Code Internal");
        validations.put(CheckInputBatchRequest::getAlgParmVal22, "Percentage of Water Consumption Char Type Code");
        validations.put(CheckInputBatchRequest::getAlgParmVal23, "Cancel Bill Message Code External");
        validations.put(CheckInputBatchRequest::getAlgParmVal24, "Cancel and Rebill Description");
        validations.put(CheckInputBatchRequest::getAlgParmVal25, "Final Bill Message Code");
        validations.put(CheckInputBatchRequest::getAlgParmVal26, "Overpayment SA Type");
        validations.put(CheckInputBatchRequest::getAlgParmVal27, "Cancel Reason Exclude List");
        validations.put(CheckInputBatchRequest::getAlgParmVal28, "Chinese Description for unit");
        validations.put(CheckInputBatchRequest::getAlgParmVal29, "Use Chinese Address Indicator");

        for (Map.Entry<Function<CheckInputBatchRequest, String>, String> entry : validations.entrySet()) {
            String value = entry.getKey().apply(request);
            if (!StringUtils.hasText(value)) {
                response.setErrorCode("253");
                response.setErrorParam(entry.getValue());
                return;
            }
        }
        // 参数赋值
        params.setDepositAdjCd(request.getAlgParmVal1().trim());
        params.setDsdChargeCd(request.getAlgParmVal2().trim());
        params.setInstFldName(request.getAlgParmVal3().trim());
        params.setSewageFldName(request.getAlgParmVal4().trim());
        params.setTesFldName(request.getAlgParmVal5().trim());
        params.setDisputeFldName(request.getAlgParmVal6().trim());
        params.setScrateTypeCd(request.getAlgParmVal7().trim());
        params.setScdfTypeCd(request.getAlgParmVal8().trim());
        params.setTesdfTypeCd(request.getAlgParmVal9().trim());
        params.setTesrateTypeCd(request.getAlgParmVal10().trim());
        params.setSqiCm(request.getAlgParmVal11().trim());
        params.setSqiGal(request.getAlgParmVal12().trim());
        params.setIgnorePrintCd(request.getAlgParmVal13().trim());
        params.setIgnoreNextCd(request.getAlgParmVal14().trim());
        params.setMaxAmtDue(Double.valueOf(request.getAlgParmVal15().trim()));
        params.setSurchgBitemCd(request.getAlgParmVal16().trim());
        params.setDsdBitemFldName(request.getAlgParmVal17().trim());
        params.setCrcvAttrTypeCd(request.getAlgParmVal18().trim());
        params.setStatuAttrTypeCd(request.getAlgParmVal19().trim());
        params.setWsdrepAttrTypeCd(request.getAlgParmVal20().trim());
        params.setCanbillMsgCd(request.getAlgParmVal21().trim());
        params.setPcwtrconAttrTypeCd(request.getAlgParmVal22().trim());
        params.setCanbillMsgCd2(request.getAlgParmVal23().trim());
        params.setCanrebillFld(request.getAlgParmVal24().trim());
        params.setFinalBillMsgCd(request.getAlgParmVal25().trim());
        params.setOverpaySdType(request.getAlgParmVal26().trim());
        params.setCanExcludeList(request.getAlgParmVal27().trim());
        params.setChiCsmUnitDescr(request.getAlgParmVal28().trim());
        params.setChiAddress(request.getAlgParmVal29().trim());

        // 特殊校验示例
        if (!Arrays.asList("Y", "N").contains(request.getAlgParmVal29())) {
            response.setErrorCode("253");
            response.setErrorParam("Use Chinese Address Indicator");
        }
    }

    private void handleException(Exception e, String errorPoint) {
        String errorMsg = String.format("EA000_CHECK_INPUT error %s: %s",
                errorPoint, e.getMessage());
        // 如果需要保留错误堆栈
        if (e instanceof SQLException) {
            // 处理数据库异常
        }
    }

    public BigDecimal fncCalcPayableAmountBill(String billId) {
        AlgorithmParameters params = new AlgorithmParameters();
        params.setDepositAdjCd("XFER-DPF");
        params.setInstFldName("BILL_INSTPL_SD");
        params.setDisputeFldName("BILL_DISPUTE_SD");
        params.setOverpaySdType("OVERPAY");
        params.setMaxAmtDue(10.0);
        params.setFinalBillMsgCd("FNB1");
        try {
            AccIdAndOpenItemSwDto account = getAccountIdAndOpenItemSw(billId);
            if (ObjectUtils.isEmpty(account)){
               throw new EmptyResultDataAccessException(1);
            }
            BigDecimal totalAmount = calculateTotalAmount(billId, account, params);

            boolean isFinalBill = checkFinalBill(billId);
            String nonConformingSw = checkNonConforming(billId, params);

            return determineFinalAmount(totalAmount, account.getOpenItemSw(),
                    isFinalBill, nonConformingSw, params);
        } catch (EmptyResultDataAccessException e) {
            return BigDecimal.ZERO;
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
    }

    @Override
    public BigDecimal zs200SetupSqlSOblbal(String billId, String accountId, AlgorithmParameters algParams) {
        return finTranMapper.calculateTotalAmount(billId, accountId, algParams);
    }

    @Override
    public BigDecimal zs170SetupSqlSBlbal(String billId, String accountId, AlgorithmParameters algParams) {
        return svcDtlMapper.calculateTotalBalance(billId, accountId, algParams);
    }

    @Override
    public BigDecimal zs380SetupSqlSDispamt(String billId, String piFieldName) {
        return billExtrMapper.zs380SetupSqlSDispamt(billId, piFieldName);
    }

    @Override
    public String ctDetailForPrinterKey0210(String piLastPayDt, BigDecimal piLastPayment, BigDecimal piDepostPaid, BigDecimal piDisputeAmt, BigDecimal piInstalmentAmt) {
        // 初始化输出字符串
        StringBuilder vOutput = new StringBuilder();

        // 处理最后支付日期
        if (piLastPayDt == null) {
            piLastPayDt = " ";
        }
        // 右填充日期到 10 位
        vOutput.append(String.format("%-10s", piLastPayDt));

        // 处理最后支付金额
        vOutput.append(formatAmount(piLastPayment));
        // 处理已支付押金
        vOutput.append(formatAmount(piDepostPaid));
        // 处理争议金额
        vOutput.append(formatAmount(piDisputeAmt));
        // 处理分期金额
        vOutput.append(formatAmount(piInstalmentAmt));

        // 返回去除尾部空格的字符串
        return vOutput.toString();
    }

    @Override
    public BigDecimal fncPayableAmountSpecialHandle(String piBillId, BigDecimal piTotAmt, BigDecimal piMaxAmtDue, String piBillMsgCode, String piFieldName1, String piFieldName2) {
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        //调用FNC_GET_OPEN_ITEM_SW函数获取表CFG_CUST_CLS中的OPEN_ITEM_SW信息
        String v_OPEN_ITEM_SW = fncGetOpenItemSw(piBillId);
        /* Determine if the Bill is a final bill. */
        //调用函数FNC_IS_FINAL_BILL.SQ100_GET_FINAL_BILL.ZS190_SETUP_SQL_S_FNLBM获取v_final_bill_sw信息
        Integer v_final_bill_cnt = billMsgMapper.selectCount(new QueryWrapper<BillMsgEntity>().lambda()
                .eq(BillMsgEntity::getBillId, piBillId)
                .eq(BillMsgEntity::getBillMsgCode, piBillMsgCode));
        String v_final_bill_sw = "N";
        if (v_final_bill_cnt > 0) {
            v_final_bill_sw = "Y";
        }
        //调用FNC_IS_NON_CONSUMPTION.SQ500_GET_NONCON.ZS630_SETUP_SQL_S_NONCON获取v_noncon_sw值
        String v_noncon_sw = zs630SetupSqlSNoncon(piBillId, piFieldName1, piFieldName2);
        if (piTotAmt.compareTo(piMaxAmtDue) >= 0
                || "Y".equals(v_OPEN_ITEM_SW)
                || "Y".equals(v_final_bill_sw)
                || "Y".equals(v_noncon_sw)) {
            if (piTotAmt.compareTo(BigDecimal.ZERO) > 0) {
                piTotAmt = piTotAmt;
                gvGlobalVariableBatch1.setW_min_amt_due_s("N");
            } else {
                piTotAmt = BigDecimal.ZERO;
                gvGlobalVariableBatch1.setW_min_amt_due_s("Y");
            }
        } else {
            piTotAmt = BigDecimal.ZERO;
            gvGlobalVariableBatch1.setW_min_amt_due_s("Y");
        }
        return piTotAmt;
    }

    @Override
    public String fncGetOpenItemSw(String piBillId) {
        return billExtrMapper.fncGetOpenItemSw(piBillId);
    }

    @Override
    public Zs490SetupSqlSSurchgBatchResponse zs490SetupSqlSSurchg(String piBillId, String piBillMsgCd, String piSurgNotFoundSw) {
        //分别查询SEQ in（1,2,3,4）、piBillMsgCd = "SURG"的MSG_PARAM_VAL值
        Zs490SetupSqlSSurchgBatchResponse response = billExtrMapper.zs490SetupSqlSSurchg(piBillId, piBillMsgCd, piSurgNotFoundSw);
        if (response == null) {
            response = new Zs490SetupSqlSSurchgBatchResponse();
            piSurgNotFoundSw = "Y";
            response.setPoSurgNotFoundSw(piSurgNotFoundSw);
        } else {
            response.setPoSurgNotFoundSw(piSurgNotFoundSw);
        }
        return response;

    }

    @Override
    public String zs650SetupSqlSCrcv(String piAttrTypeCd) {
        return billExtrMapper.zs650SetupSqlSCrcv(piAttrTypeCd);
    }

    @Override
    public String ctDetailForPrinterKey5000(String dtOfIssue, String chargeNo, BigDecimal amountDue, String fiveSurchargeDt, BigDecimal fiveSurcharge, String tenSurchargeDt, BigDecimal tenSurcharge, String crcNo, int serialNo, String slipType) {
        StringBuilder vOutput = new StringBuilder();

        // 处理 dtOfIssue
        vOutput.append(rpad(dtOfIssue, 10, ' '));

        // 处理 chargeNo
        vOutput.append(rpad(chargeNo, 13, ' '));

        // 处理 amountDue
        vOutput.append(formatAmount(amountDue));

        // 处理 fiveSurchargeDt
        if (fiveSurchargeDt == null) {
            fiveSurchargeDt = " ";
        }
        vOutput.append(rpad(fiveSurchargeDt, 10, ' '));

        // 处理 fiveSurcharge
        vOutput.append(formatAmount(fiveSurcharge));

        // 处理 tenSurchargeDt
        if (tenSurchargeDt == null) {
            tenSurchargeDt = " ";
        }
        vOutput.append(rpad(tenSurchargeDt, 10, ' '));

        // 处理 tenSurcharge
        vOutput.append(formatAmount(tenSurcharge));

        // 处理 crcNo
        vOutput.append(rpad(crcNo, 3, ' '));

        // 处理 serialNo
        vOutput.append(String.format("%06d", Math.abs(serialNo)));

        // 处理 slipType
        vOutput.append(rpad(slipType, 1, ' '));

        return vOutput.toString().trim();
    }

    @Override
    public void mx010WriteExtractData(String piBillId, String piExtrLine, String piUserId, Date piProcessDttm) {
        // 定义日期格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
        // 格式化日期
        String formattedDate = sdf.format(piProcessDttm);
        // 拼接字符串
        String pi_process_dttm = formattedDate + ".000000";
        BillExtrEntity entity2 = new BillExtrEntity();
        entity2.setBillId(piBillId);
        entity2.setExtrLine(piExtrLine);
        entity2.setUserid(piUserId);
        entity2.setProcDate(pi_process_dttm);
        entity2.setCreatedDate(new Date());
        entity2.setCreatedBy("SYS");
        entity2.setModifiedBy("SYS");
        entity2.setModifiedDate(new Date());
        entity2.setTimestamp(new Date());
        try {
            billExtrMapper.insert(entity2);
        } catch (Throwable cause) {

        }
    }

    @Override
    public void ma050BillMessages(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piBillId, String piLanguageCd) {
        AlgorithmParameters params = algorithmParams.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        if ("0".equals(gvBillExtractionHeaderBatch1.getSortKeyPreId())) {
            gvBillExtractionHeaderBatch1.setSortKeyPreId("1");
        }
        //调用SQ110_GET_NEXT_BILLMSG.ZS160_SETUP_SQL_S_BILLMSG存储过程
        List<zs160SetupSqlSBillmsgBatchRespomse> auto = zs160SetupSqlSBillmsg(piBillId, params.getSurchgBitemCd(), "AUTO", piLanguageCd);
        int wMsgNo = 0;
        for (zs160SetupSqlSBillmsgBatchRespomse billMessage : auto) {
            String v_MSG_PRIORITY_FLG = billMessage.getBillMsgCode().substring(0, 2);
            wMsgNo++;
            // 设置全局变量的值
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("60");
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("4000");

            //调用MA060_EXPAND_BILL_MSG存储过程
            String v_MSG_ON_BILL = ma060ExpandBillMsg(piBillId, billMessage.getBillMsgCode(), billMessage.getMsgOnBill());

            String vMsgNo = String.format("%02d", wMsgNo);
            //调用函数ct_detail_for_printer_key_4000获取v_bp_extr_dtl值
            String v_bp_extr_dtl = ctDetailForPrinterKey4000(
                    v_MSG_PRIORITY_FLG,
                    vMsgNo,
                    v_MSG_ON_BILL
            );
            //调用construct_bill_extr_line函数获取v_bp_extr_lines值
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        }
    }

    public static String ctDetailForPrinterKey4000(
            String piMsgPriorityFlg,
            String piMsgNo,
            String piMsgOnBill) {
        String v_output = rpad(piMsgPriorityFlg, 2, ' ') +
                rpad("", 8, ' ') +
                lpad(piMsgNo, 2, '0') +
                substrb(piMsgOnBill, 254);
        return v_output.trim();
    }

    private static String lpad(String input, int length, char padChar) {
        if (input == null) {
            input = "";
        }
        StringBuilder sb = new StringBuilder();
        while (sb.length() + input.length() < length) {
            sb.append(padChar);
        }
        sb.append(input);
        return sb.toString();
    }

    private static String substrb(String input, int length) {
        if (input == null) {
            input = "";
        }
        if (input.length() >= length) {
            return input.substring(0, length);
        } else {
            StringBuilder sb = new StringBuilder(input);
            while (sb.length() < length) {
                sb.append(' ');
            }
            return sb.toString();
        }
    }

    @Override
    public List<zs160SetupSqlSBillmsgBatchRespomse> zs160SetupSqlSBillmsg(String piBillId, String piSurchgBitemCd, String piApayBitemCd, String piLanguageCd) {
        return billExtrMapper.zs160SetupSqlSBillmsg(piBillId, piSurchgBitemCd, piApayBitemCd, piLanguageCd);
    }

    @Override
    public String ma060ExpandBillMsg(String piBillId, String piBillMsgCd, String piMsgOnBill) {
        String[] v_msg_parm = new String[10];
        //调用SQ120_GET_MSG_PARMS.ZS180_SETUP_SQL_S_BMSGPRM
        List<Zs180SetupSqlSBmsgprmBatchResponse> msgParams = zs180SetupSqlSBmsgprm(piBillId, piBillMsgCd);
        for (Zs180SetupSqlSBmsgprmBatchResponse param : msgParams) {
            int seqNum = param.getSeq();
            if (seqNum >= 1 && seqNum <= 9) {
                v_msg_parm[seqNum - 1] = param.getMsgParamVal();
            }
        }
        //调用PA100_SUB_PARM_VALUES
        return pa100SubParmValues(piMsgOnBill, Arrays.asList(v_msg_parm));
    }

    @Override
    public String pa100SubParmValues(String piString, List<String> piMsgParm) {
//        gvGlobalVariableBatch.get();
        String tmp_string = piString;
        for (int i = 0; i < piMsgParm.size(); i++) {
            int index = i + 1;
            String parmValue = piMsgParm.get(i);
            if (parmValue == null || parmValue.trim().isEmpty()) {
                tmp_string = tmp_string.replace("%" + index + " ", "").replace("%" + index, "");
            } else {
                tmp_string = tmp_string.replace("%" + index + " ", parmValue.trim()).replace("%" + index, parmValue.trim());
            }
        }
        return tmp_string;
    }

    @Override
    public Md010GetNosBillmsgBatchResponse md010GetNosBillmsg(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piBillId, String piLanguageCd) {
        AlgorithmParameters params = algorithmParams.get();
        Md010GetNosBillmsgBatchResponse response = new Md010GetNosBillmsgBatchResponse();
        List<String> dsddfBitemCd = new ArrayList<>();
        List<String> tesrateBitemCd = new ArrayList<>();
        List<String> tesdfBitemCd = new ArrayList<>();
        List<String> dfbitemCd = new ArrayList<>();

        //调用SQ420_GET_MSGFLD.ZS510_SETUP_SQL_S_MSGFLD存储过程
        List<String> v_msgfld_cursor = zs510SetupSqlSMsgfld(params.getDsdBitemFldName(), "1", piLanguageCd == null ? "ENG" : piLanguageCd);
        for (String descr : v_msgfld_cursor) {
            if (descr != null && descr.trim().getBytes().length > 4) {
                // 模拟 ZZ000_SETUP_SQL_I_CI_MSG 存储过程调用
                zz000SetupSqlICiMsg(piBillId, "30", "v_DESCR len>4:" + descr);
            }
            dsddfBitemCd.add(descr);
            dfbitemCd.add(descr);
        }
        // 模拟 SQ420_GET_MSGFLD 存储过程调用，参数 '4'
        v_msgfld_cursor = zs510SetupSqlSMsgfld(params.getDsdBitemFldName(), "4", piLanguageCd);
        for (String descr : v_msgfld_cursor) {
            tesrateBitemCd.add(descr);
            dfbitemCd.add(descr);
        }
        // 模拟 SQ420_GET_MSGFLD 存储过程调用，参数 '4'
        v_msgfld_cursor = zs510SetupSqlSMsgfld(params.getDsdBitemFldName(), "3", piLanguageCd);
        for (String descr : v_msgfld_cursor) {
            tesdfBitemCd.add(descr);
            dfbitemCd.add(descr);
        }
        response.setDsddfBitemCd(dsddfBitemCd);
        response.setTesrateBitemCd(tesrateBitemCd);
        response.setTesdfBitemCd(tesdfBitemCd);
        response.setDfbitemCd(dfbitemCd);
        return response;
    }

    @Override
    public List<String> zs510SetupSqlSMsgfld(String piFieldName, String piFieldValue, String piLanguageCd) {
        return billExtrMapper.zs510SetupSqlSMsgfld(piFieldName, piFieldValue, piLanguageCd == null ? "ENG" : piLanguageCd);
    }

    public void mc010PremiseGroup(Mc010PremiseGroupBatchRequest request,
                                  List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                  List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
                                  List<BitemWithReadAndQty> bitemReadList,
                                  List<BitemCalcBdWithAttr> bitemCalcBdList,
                                  List<FinTranWithAdj> finTranWithAdjList,
                                  List<RateWithDtlAndCrit> rateList,
                                  List<SvcDtlContractValEntity> contractValList) {
        // 准备消息
        List<String> bitemIds = new ArrayList<>();
        for (BitemWithReadAndQty bitem : bitemReadList) {
            bitemIds.add(bitem.getBitemId());
        }
        List<BitemMsgWithMsg> bitemMsgWithMsgList;
        if (bitemIds.isEmpty()) {
            bitemMsgWithMsgList = new ArrayList<>();
        } else {
            bitemMsgWithMsgList = billExtrMapper.selectBitemMsgWithMsg(bitemIds);
        }
        //调用MQ010_GET_LIST1存储过程
        StringBuilder supplyNatureHolder = new StringBuilder();
        List<Mq010GetList1BtachResponse> v_list1 = mq010GetList1(request,
            svcDtlWithTypeAndBalList, prevSvcDtlWithTypeAndBalList,
            bitemReadList, bitemCalcBdList, finTranWithAdjList,
            rateList, contractValList, supplyNatureHolder);
        //调用MQ020_GET_LIST2存储过程
        Mq020GetList2BatchResponse mq020Response = mq020GetList2(request, svcDtlWithTypeAndBalList, finTranWithAdjList);
        Integer v_dummy_pre_cnt = mq020Response.getPoDummyPreCnt();
        List<Zs670SetupSqlSList2BatchDto> v_list2 = mq020Response.getPoList2();
        //调用MQ030_GET_LIST3存储过程
        List<SvcDtlWithTypeAndBal> svcList3 = new ArrayList<>();
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            boolean found = false;
            for (Mq010GetList1BtachResponse svcInList1 : v_list1) {
                if (svc.getSvcId().equals(svcInList1.getSvcId())) {
                    found = true;
                    break;
                }
            }
            if (!found && !svc.getSvcTypeCode().endsWith("-D")) {
                svcList3.add(svc);
            }
        }
        if (!svcList3.isEmpty()) {
            mq030GetList3(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(), request.getPiBillRoutingRow(),
                request.getPiLanguageCd(), request.getPiBillType(), request.getPiPrebillId(), v_dummy_pre_cnt, svcList3);
        }
        //Create Service Point Bill Message Record (3500)
        //调用ME010_CREATE_RECORD_3500存储过程
        me010CreateRecord3500(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                request.getPiLanguageCd(), v_list1, v_list2, request.getPiDfbitemCd(),
                svcDtlWithTypeAndBalList, bitemReadList, bitemMsgWithMsgList, finTranWithAdjList);
        //Create Bill Segment Calc Header Record (3200)
        //调用ME020_CREATE_RECORD_3200存储过程
        Map<String, Object> me020Map = me020CreateRecord3200(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                request.getPiLanguageCd(), v_list1, v_list2, svcDtlWithTypeAndBalList, bitemReadList, finTranWithAdjList);
        v_list1 = (List<Mq010GetList1BtachResponse>) me020Map.get("po_list1");
        v_list2 = (List<Zs670SetupSqlSList2BatchDto>) me020Map.get("po_list2");
        //Create Bill Segment Calc Line Record (3210)
        //调用ME030_CREATE_RECORD_3210存储过程
        Map<String, Object> me030Map = me030CreateRecord3210(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                request.getPiLanguageCd(), v_list1, v_list2, supplyNatureHolder.toString());
        v_list1 = (List<Mq010GetList1BtachResponse>) me030Map.get("po_list1");
        v_list2 = (List<Zs670SetupSqlSList2BatchDto>) me030Map.get("po_list2");
        //调用ME040_CREATE_RECORD_3300存储过程
        Map<String, Object> me040Map = me040CreateRecord3300(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                request.getPiLanguageCd(), v_list1, v_list2, svcDtlWithTypeAndBalList, finTranWithAdjList);
        v_list1 = (List<Mq010GetList1BtachResponse>) me040Map.get("po_list1");
        v_list2 = (List<Zs670SetupSqlSList2BatchDto>) me040Map.get("po_list2");
        //调用ME050_CREATE_RECORD_3100存储过程
        me050CreateRecord3100(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                request.getPiLanguageCd(), v_list1, v_list2, svcDtlWithTypeAndBalList, finTranWithAdjList);
        //调用ME060_CREATE_RECORD_2200存储过程
        me060CreateRecord2200(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(), svcDtlWithTypeAndBalList);
        //调用ME070_CREATE_RECORD_2300存储过程
        me070CreateRecord2300(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                bitemReadList, finTranWithAdjList);
    }


    public List<Mq010GetList1BtachResponse> mq010GetList1(
            Mc010PremiseGroupBatchRequest request,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
            List<BitemWithReadAndQty> bitemReadList,
            List<BitemCalcBdWithAttr> bitemCalcBdList,
            List<FinTranWithAdj> finTranWithAdjList,
            List<RateWithDtlAndCrit> rateList,
            List<SvcDtlContractValEntity> contractValList,
            StringBuilder supplyNatureHolder) {
        Set<String> bitemIds = new HashSet<>();
        Set<String> svcIds = new HashSet<>();
        bitemReadList.forEach(item -> {
            bitemIds.add(item.getBitemId());
            svcIds.add(item.getSvcId());
        });
        svcDtlWithTypeAndBalList.forEach(item -> {
            svcIds.add(item.getSvcId());
        });
        Set<String> rateIds = new HashSet<>();
        for (BitemWithReadAndQty bitem : bitemReadList) {
            if (bitem.getRateId() != null) {
                rateIds.add(bitem.getRateId());
            }
        }
        List<Mq010GetList1BtachResponse> list1 = new ArrayList<>();
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        int v_list1_cnt = 0;
        String v_temp_pre_id = null;
        String v_temp_svc_id = null;
        //调用SQ550_GET_LIST1.ZS660_SETUP_SQL_S_LIST1存储过程
        List<Zs660SetupSqlSList1BatchResponse> zs660Responses = zs660SetupSqlSList1(request.getPiBillRow().getBillId(),
                request.getPiLanguageCd(), params.getInstFldName(),
                params.getDisputeFldName(), params.getDepositAdjCd(),
                svcDtlWithTypeAndBalList, finTranWithAdjList);
        for (Zs660SetupSqlSList1BatchResponse Zs660Response : zs660Responses) {
            if (v_list1_cnt >= gvGlobalVariableBatch1.getW_max_cnt()) {
                break;
            }
            //调用XQ050_INSERT_SA_LIST.ZS941_SETUP_SQL_I_TEMPSA存储过程
            try {
                billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(Zs660Response.getSvcId(), "1SD"));
            } catch (Throwable cause) {
                // nothing to do
            }
            v_list1_cnt++;
            Mq010GetList1BtachResponse response = new Mq010GetList1BtachResponse();
            response.setPreId(Zs660Response.getGeoAddressId());
            response.setSvcId(Zs660Response.getSvcId());
            response.setSpecialRoleFlg(Zs660Response.getSpecialRoleInd());
            response.setDfltDescrOnBill(Zs660Response.getSvcTypeDfltDescOnBill());
            if ("BONLROUT".equals(request.getPiBillRoutingRow().getBatchJobCode())) {
                response.setBillPrtPrioFlg(String.format("%04d", v_list1_cnt).substring(0, 2));
            } else {
                response.setBillPrtPrioFlg(String.format("%02d", v_list1_cnt).substring(0, 2));
            }
            response.setSubTotalSw("N");
            list1.add(response);
        }
        List<Mq010GetList1BtachResponse> wList1SdListPre = new ArrayList<>();

        Set<String> existingPremises = new HashSet<>();
        for (int wIdx1 = 1; wIdx1 <= list1.size(); wIdx1++) {
            Mq010GetList1BtachResponse vList1Element = list1.get(wIdx1 - 1);
            if (v_temp_pre_id == null || !v_temp_pre_id.equals(vList1Element.getPreId()) || v_temp_pre_id.trim().isEmpty()) {
                int vTempIndex = 0;
                //调用XQ051_DELETE_SA_LIST存储过程删除相关数据
                billExtrSvcDtlGttMapper.delete(Wrappers.<BillExtrSvcDtlGttEntity>lambdaQuery()
                        .eq(BillExtrSvcDtlGttEntity::getListType, "1PM")
                        .eq(BillExtrSvcDtlGttEntity::getSvcId, vList1Element.getSvcId()));
//                xq051DeleteSaList("1PM");
                wList1SdListPre.clear();
                for (int vW2Index = 1; vW2Index <= list1.size(); vW2Index++) {
                    Mq010GetList1BtachResponse vW2Element = list1.get(vW2Index - 1);
                    if ((vW2Element.getPreId() == null || vList1Element.getPreId() == null) ||
                            (vW2Element.getPreId() != null && vList1Element.getPreId() != null &&
                            vW2Element.getPreId().equals(vList1Element.getPreId()))) {
                        wList1SdListPre.add(new Mq010GetList1BtachResponse());
                        vTempIndex++;
                        wList1SdListPre.get(vTempIndex - 1).setSvcId(vW2Element.getSvcId());
                        //调用XQ050_INSERT_SA_LIST存储过程插入数据
                        //listDataService.xq050InsertSaList("1PM", vW2Element.getSvcId());
                        try {
                            billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(vW2Element.getSvcId(), "1PM"));
                        } catch (Throwable cause) {

                        }
                    }
                }
                v_temp_pre_id = vList1Element.getPreId();
                String preId = v_temp_pre_id;
                if (preId == null) {
                    preId = "0000000001";
                    v_temp_pre_id = preId;
                }
                if (existingPremises.contains(preId)) {
                    continue;
                } else {
                    existingPremises.add(preId);
                }
                v_temp_svc_id = vList1Element.getSvcId();
                String vAddress1 = null;
                String vAddress2 = null;
                String vAddress3 = null;
                String vAddress4 = null;
                if ("ZHT".equals(request.getPiLanguageCd()) || "CHI".equals(request.getPiLanguageCd()) && "Y".equals(params.getChiAddress())) {
                    //调用SQ490_GET_ALTPREM.ZS580_SETUP_SQL_S_ALTPREM存储过程获取PremiseZht表地址信息
                    String v_alt_prem_found_sw = "N";
                    PremiseZhtEntity premiseZhtEntity = premiseZhtMapper.selectById(v_temp_pre_id);
                    if (premiseZhtEntity != null) {
                        v_alt_prem_found_sw = "Y";
                        vAddress1 = premiseZhtEntity.getAddrLn1();
                        vAddress2 = premiseZhtEntity.getAddrLn2();
                        vAddress3 = premiseZhtEntity.getAddrLn3();
                        vAddress4 = premiseZhtEntity.getAddrLn4();
                    }
                    if ("N".equals(v_alt_prem_found_sw)) {
                        //调用SQ240_GET_SPPREM.ZS330_SETUP_SQL_S_SPPREM存储过程获取PREMISE表的地址信息
                        GeoAddressntity geoAddressntity = premiseMapper.selectById(v_temp_pre_id);
                        if (geoAddressntity != null) {
                            vAddress1 = geoAddressntity.getAddrLn1();
                            vAddress2 = geoAddressntity.getAddrLn2();
                            vAddress3 = geoAddressntity.getAddrLn3();
                            vAddress4 = geoAddressntity.getAddrLn4();
                        }
                    }
                } else {
                    //调用SQ240_GET_SPPREM.ZS330_SETUP_SQL_S_SPPREM存储过程PREMISE表地址信息
                    GeoAddressntity geoAddressntity = premiseMapper.selectById(v_temp_pre_id);
                    if (geoAddressntity != null) {
                        vAddress1 = geoAddressntity.getAddrLn1();
                        vAddress2 = geoAddressntity.getAddrLn2();
                        vAddress3 = geoAddressntity.getAddrLn3();
                        vAddress4 = geoAddressntity.getAddrLn4();
                    }
                }
                //调用SQ140_GET_MTRNBR.ZS130_SETUP_SQL_S_MTRNBR存储过程
                String v_serial_nbr = zs130SetupSqlSMtrnbr(v_temp_pre_id, request.getPiBillRow().getCmpltDt(), v_temp_svc_id);
                vList1Element.setPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
                gvBillExtractionHeaderBatch1.setSortKeyPreId(v_temp_pre_id);
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("10");
                /*!
                ** 提取2000部分数据，主要包括premise address
                */
                //调用ctDetailForPrinterKey2000方法
                String v_bp_extr_dtl = ctDetailForPrinterKey2000(v_temp_pre_id, "Y", vAddress1, vAddress2, vAddress3, vAddress4, v_serial_nbr);
                //调用construct_bill_extr_line函数
                gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2000");
                String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
                mx010WriteExtractData(request.getPiBillRow().getBillId(), v_bp_extr_lines, request.getPiUserId(), request.getPiProcessDttm());
                //调用MR080_CREATE_RECORD_2100A存储过程添加2100记录
                String supplyNature = mr080CreateRecord2100a(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(), v_temp_pre_id,
                    request.getPiLanguageCd(), request.getPiDsddfBmsgCd(), request.getPiTesrateBmsgCd(), request.getPiTesdfBmsgCd(),
                    svcDtlWithTypeAndBalList, bitemReadList, bitemCalcBdList,
                    finTranWithAdjList, rateList, contractValList);
                supplyNatureHolder.append(supplyNature);
                /*!
                ** 提取2400部分数据，主要包括水表读数。
                */
                Set<String> premiseIds = new HashSet<>();
                for (BitemWithReadAndQty bitem : bitemReadList) {
                    premiseIds.add(bitem.getGeoAddressId());
                }
                if (!premiseIds.isEmpty()) {
                    List<Map<String, Object>> cofcList = billExtrMapper.selectCofc(premiseIds);
                    mr040GetMtrRead(request.getPiUserId(), request.getPiProcessDttm(),
                            request.getPiBillRow(), v_temp_pre_id, "1SD",
                            bitemReadList, cofcList);
                }
                // 调用MJ010_GET_LIST1_SPBAL存储过程
                List<BillExtrSvcDtlGttEntity> gttList;
                if (svcIds.isEmpty()) {
                    gttList = new ArrayList<>();
                } else {
                    gttList = billExtrSvcDtlGttMapper.selectList(new QueryWrapper<BillExtrSvcDtlGttEntity>().lambda().
                        in(BillExtrSvcDtlGttEntity::getSvcId, svcIds));
                }
                // 附加
                for (String svcId : svcIds) {
                    boolean found = false;
                    for (BillExtrSvcDtlGttEntity gtt : gttList) {
                        if (gtt.getSvcId().equals(svcId)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        BillExtrSvcDtlGttEntity gtt = new BillExtrSvcDtlGttEntity();
                        gtt.setSvcId(svcId);
                        gtt.setListType("XX");
                        gttList.add(gtt);
                    }
                }
                mj010GetList1Spbal(
                    request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                    request.getPiBillType(), "1PM"/* not used */, request.getPiPrebillId(),
                    finTranWithAdjList, svcDtlWithTypeAndBalList, prevSvcDtlWithTypeAndBalList,
                    gttList, bitemReadList, v_temp_pre_id);
                // Bill message for cancelled adjustments bill segments:
                // 调用MG010_CAN_BILL_MESSAGE存储过程
                // 存在取消项的svc所属的premise才会添加信息
                String getGeoAddressId = null;
                for (FinTranWithAdj ft : finTranWithAdjList) {
                    if (!"BX".equals(ft.getFinTranTypeInd())) {
                        continue;
                    }
                    for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
                        if (svc.getSvcId().equals(ft.getSvcId()) &&
                            premiseEquals(svc.getGeoAddressId() == null ? "0000000001" : svc.getGeoAddressId(), v_temp_pre_id)) {
                            getGeoAddressId = v_temp_pre_id;
                            break;
                        }
                    }
                }
                if (premiseEquals(getGeoAddressId, v_temp_pre_id)) {
                    // 存在BX
                    mg010CanBillMessage(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(),
                        v_temp_pre_id, request.getPiLanguageCd(), "1PM");
                }
                //调用MO010_END_SP存储过程
                mo010EndSp(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow());
            } else {
                vList1Element.setPrePrintPriority("00");
            }
            v_temp_svc_id = vList1Element.getSvcId();
        }
        return list1;
    }

    public List<Zs660SetupSqlSList1BatchResponse> zs660SetupSqlSList1(
            String piBillId, String piLanguageCd, String piFieldName1,
            String piFieldName2, String piAdjTypeCd,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList, List<FinTranWithAdj> finTranWithAdjList) {
//        List<Zs660SetupSqlSList1BatchResponse> retVal = new ArrayList<>();
//        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
//            boolean found = false;
            // TODO
//            AND SD.SVC_TYPE_CODE NOT IN (
//                    SELECT
//                    VAL
//                    FROM
//                    CFG_LKUP_CODE
//                    WHERE
//                    LKUP_TYPE = #{piFieldName1}
//            OR LKUP_TYPE = #{piFieldName2}
//            for (FinTranWithAdj finTran : finTranWithAdjList) {
//                if (!svc.getSvcId().equals(finTran.getSvcId())) {
//                    continue;
//                }
//                if (!"Y".equals(finTran.getFreezeSw())) {
//                    continue;
//                }
//                if (!"Y".equals(finTran.getShowOnBillSw())) {
//                    continue;
//                }
//                if (!"BS".equals(finTran.getFinTranTypeInd()) &&
//                    !"BX".equals(finTran.getFinTranTypeInd()) &&
//                    !"AS".equals(finTran.getFinTranTypeInd()) &&
//                    !"AX".equals(finTran.getFinTranTypeInd())) {
//                    continue;
//                }
//                if ("AS".equals(finTran.getFinTranTypeInd()) ||
//                    "AX".equals(finTran.getFinTranTypeInd())) {
//                    if (piAdjTypeCd.equals(finTran.getAdjTypeCode())) {
//                        continue;
//                    }
//                }
//                found = true;
//            }
//            if (found) {
//                Zs660SetupSqlSList1BatchResponse row = new Zs660SetupSqlSList1BatchResponse();
//                row.setPremiseId(svc.getPremiseId());
//                row.setSvcId(svc.getSvcId());
//                row.setBillPrintPrio(svc.getBillPrintPrio());
//                row.setSpecialRoleInd(svc.getSpecialRoleInd());
//                if ("ENG".equals(piLanguageCd)) {
//                    row.setSvcTypeDfltDescOnBill(svc.getSvcTypeDfltDescOnBill());
//                } else {
//                    row.setSvcTypeDfltDescOnBill(svc.getSvcTypeDfltDescOnBillTc());
//                }
//                retVal.add(row);
//            }
//        }
//        return retVal;
        return billExtrMapper.zs660SetupSqlSList1(piBillId, piLanguageCd, piFieldName1, piFieldName2, piAdjTypeCd);
    }

    @Override
    public String zs130SetupSqlSMtrnbr(String piPremiseId, Date piCompleteDttm, String piSvcId) {
        return "XXXXXXXXXX";
        // return billExtrMapper.zs130SetupSqlSMtrnbr(piPremiseId, piCompleteDttm, piSvcId);
    }

    public String mr080CreateRecord2100a(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piPremiseId, String piLanguageCd,
                                         List<String> piDsddfBmsgCd, List<String> piTesrateBmsgCd, List<String> piTesdfBmsgCd,
                                         List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                         List<BitemWithReadAndQty> bitemWithReadAndQtyList,
                                         List<BitemCalcBdWithAttr> bitemCalcBdWithAttrList,
                                         List<FinTranWithAdj> finTranWithAdjList,
                                         List<RateWithDtlAndCrit> rateList,
                                         List<SvcDtlContractValEntity> contractValEntityList) {
        AlgorithmParameters params = algorithmParams.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        String v_DSDDF_VAL = null;
        String v_DSDRATE_VAL = null;
        String v_TESRATE_VAL = null;
        String v_TESDF_VAL = null;
        String v_znmfn_formated_amt = null;
        String retVal = "";

        String v_HYBRID_ACCT = "N";
        //调用SQ310_GET_SICCD.ZS410_SETUP_SQL_S_SICCD存储过程
        Zs410SetupSqlSSiccdBatchResponse zs410Response = null;
        try {
            List<Zs410SetupSqlSSiccdBatchResponse> items = zs410SetupSqlSSiccd(piBillRow.getBillId(), piPremiseId, piBillRow.getAccountId(), piLanguageCd);
            if (items != null && !items.isEmpty()) {
                zs410Response = items.get(0);
            }
        } catch (Exception e) {
            v_HYBRID_ACCT = "Y";
        }
        if (zs410Response == null) {
            zs410Response = new Zs410SetupSqlSSiccdBatchResponse();
        }
        String v_descr = zs410Response.getPoDescr();
        retVal = v_descr;
        String v_hsic_cd = zs410Response.getPoHsicCd();
        if ("N".equals(v_HYBRID_ACCT)) {
            //调用SQ770_GET_S_SACOBF.ZS880_SETUP_SQL_S_SACOBF存储过程
            List<Zs880SetupSqlSSacobfBatchResponse> zs880Responses = zs880SetupSqlSSacobf_improved(
                piBillRow.getBillId(), piPremiseId,
                bitemCalcBdWithAttrList, bitemWithReadAndQtyList,
                rateList, finTranWithAdjList,
                svcDtlWithTypeAndBalList, contractValEntityList);
            for (Zs880SetupSqlSSacobfBatchResponse zs880Response : zs880Responses) {
                if (zs880Response.getAttrTypeCode() == null) {
                    zs880Response.setAttrTypeCode("");
                }
                if ("S".equals(zs880Response.getValType())) {
                    if (zs880Response.getHsicCode().trim().equals(zs410Response.getPoHsicCd().trim())) {
                        if (zs880Response.getAttrTypeCode().trim().equals(params.getScdfTypeCd())) {
                            v_DSDDF_VAL = padRight(zs880Response.getChgRateVal().divide(BigDecimal.valueOf(100), 3, RoundingMode.DOWN).toPlainString(), 5);
                            v_znmfn_formated_amt = v_DSDDF_VAL;
                        } else if (zs880Response.getAttrTypeCode().trim().equals(params.getTesdfTypeCd())) {
                            v_TESDF_VAL = padRight(zs880Response.getChgRateVal().divide(BigDecimal.valueOf(100), 3, RoundingMode.DOWN).toPlainString(), 5);
                            v_znmfn_formated_amt = v_TESDF_VAL;
                        } else if (zs880Response.getAttrTypeCode().trim().equals(params.getTesrateTypeCd())) {
                            v_TESRATE_VAL = padRight(zs880Response.getChgRateVal().setScale(2, RoundingMode.DOWN).toPlainString(), 5);
                            v_znmfn_formated_amt = v_TESRATE_VAL;
                        } else if (zs880Response.getAttrTypeCode().trim().equals(params.getScrateTypeCd())) {
                            v_DSDRATE_VAL = padRight(zs880Response.getChgRateVal().setScale(2, RoundingMode.DOWN).toPlainString(), 5);
                            v_znmfn_formated_amt = v_DSDRATE_VAL;
                        }
                    }
                }
                if ("C".equals(zs880Response.getValType())) {
                    if (zs880Response.getAttrTypeCode().trim().equals(params.getScdfTypeCd().trim())) {
                        if (zs880Response.getHsicCode().trim().equals(zs410Response.getPoHsicCd().trim())) {
                            v_DSDDF_VAL = padRight(zs880Response.getChgRateVal().divide(BigDecimal.valueOf(100), 3, RoundingMode.DOWN).toPlainString().substring(0, 5), 5);
                        }
                        v_znmfn_formated_amt = v_DSDDF_VAL;
                        //调用SQ810_GET_END_DATE.ZS960_SETUP_SQL_S_SACOED存储过程
                        String v_contract_end_dt = zs960SetupSqlSSacoed(piBillRow.getBillId(), piPremiseId, zs880Response.getBillParamCode());
                        if (v_contract_end_dt != null && !"3000-01-01".equals(v_contract_end_dt)) {
                            for (String dsddfBmsgCd : piDsddfBmsgCd) {
                                //调用MA070_ADD_BSEG_MESSAGE存储过程
                                String v_msg_on_bill = ma070AddBsegMessage(piUserId, piProcessDttm, piBillRow, piPremiseId, dsddfBmsgCd, piLanguageCd, v_znmfn_formated_amt, v_contract_end_dt);
                            }
                        }
                    } else if (zs880Response.getAttrTypeCode().trim().equals(params.getTesdfTypeCd().trim())) {
                        if (zs880Response.getHsicCode().trim().equals(zs410Response.getPoHsicCd().trim())) {
                            v_DSDDF_VAL = padRight(zs880Response.getChgRateVal().divide(BigDecimal.valueOf(100), 3, RoundingMode.DOWN).toPlainString().substring(0, 5), 5);
                        }
                        v_znmfn_formated_amt = v_DSDDF_VAL;
                        //调用SQ810_GET_END_DATE.ZS960_SETUP_SQL_S_SACOED存储过程
                        String v_contract_end_dt = zs960SetupSqlSSacoed(piBillRow.getBillId(), piPremiseId, zs880Response.getBillParamCode());
                        if (v_contract_end_dt != null && !"3000-01-01".equals(v_contract_end_dt)) {
                            for (String tesdfBmsgCd : piTesdfBmsgCd) {
                                //调用MA070_ADD_BSEG_MESSAGE存储过程
                                String v_msg_on_bill = ma070AddBsegMessage(piUserId, piProcessDttm, piBillRow, piPremiseId, tesdfBmsgCd, piLanguageCd, v_znmfn_formated_amt, v_contract_end_dt);
                            }
                        }
                    } else if (zs880Response.getAttrTypeCode().trim().equals(params.getTesrateTypeCd().trim())) {
                        String val = zs880Response.getChgRateVal().setScale(2, RoundingMode.DOWN).toPlainString();
                        if (val.length() > 5) {
                            val = val.substring(0, 5);
                        }
                        v_TESRATE_VAL = padRight(val, 5);
                        v_znmfn_formated_amt = v_TESRATE_VAL;
                        //调用SQ810_GET_END_DATE.ZS960_SETUP_SQL_S_SACOED存储过程
                        String v_contract_end_dt = zs960SetupSqlSSacoed(piBillRow.getBillId(), piPremiseId, zs880Response.getBillParamCode());
                        if (v_contract_end_dt != null && !"3000-01-01".equals(v_contract_end_dt)) {
                            for (String tesrateBmsgCd : piTesrateBmsgCd) {
                                //调用MA070_ADD_BSEG_MESSAGE存储过程
                                String v_msg_on_bill = ma070AddBsegMessage(piUserId, piProcessDttm, piBillRow, piPremiseId, tesrateBmsgCd, piLanguageCd, v_znmfn_formated_amt, v_contract_end_dt);
                            }
                        }
                    } else if (zs880Response.getAttrTypeCode().trim().equals(params.getScrateTypeCd().trim())) {
                        if (zs880Response.getHsicCode().trim().equals(zs410Response.getPoHsicCd().trim())) {
                            v_DSDRATE_VAL = padRight(zs880Response.getChgRateVal().setScale(2, RoundingMode.DOWN).toPlainString().substring(0, 5), 5);
                        }
                    }
                }
            }
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2100");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("20");
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");

            //调用ct_detail_for_printer_key_2100函数
            String v_bp_extr_dtl = ctDetailForPrinterKey2100(v_descr, v_DSDRATE_VAL, v_DSDDF_VAL, v_TESDF_VAL, v_TESRATE_VAL, v_hsic_cd);
            //调用construct_bill_extr_line函数
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        }
        return retVal;
    }

    private static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);
    }

    @Override
    public List<Zs410SetupSqlSSiccdBatchResponse> zs410SetupSqlSSiccd(String piBillId, String piPremiseId, String piAcctId, String piLanguageCd) {
        return billExtrMapper.zs410SetupSqlSSiccd(piBillId, piPremiseId, piAcctId, piLanguageCd);
    }

    public List<Zs880SetupSqlSSacobfBatchResponse> zs880SetupSqlSSacobf(String piBillId,
                                                                        String piPremiseId,
                                                                        Set<String> rateIds,
                                                                        List<BitemWithReadAndQty> bitemWithReadAndQtyList,
                                                                        List<FinTranWithAdj> finTranWithAdjList) {
        Set<String> bitemIds = new HashSet<>();
        Set<String> svcIds = new HashSet<>();
        for (BitemWithReadAndQty bitem : bitemWithReadAndQtyList) {
            if (bitem.getCalcStartDate() != null) {
                bitemIds.add(bitem.getBitemId());
            }
            if (bitem.getSvcId() != null) {
                svcIds.add(bitem.getSvcId());
            }
        }
        for (FinTranWithAdj fin : finTranWithAdjList) {
            if ("BX".equals(fin.getFinTranTypeInd())) {
                bitemIds.remove(fin.getFinTranTypeId());
            }
        }
        if (bitemIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<Zs880SetupSqlSSacobfBatchResponse> list1 = billExtrMapper.zs880SetupSqlSSacobf_01(piBillId, piPremiseId, rateIds, bitemIds);
        List<Zs880SetupSqlSSacobfBatchResponse> list2 = billExtrMapper.zs880SetupSqlSSacobf_02(piBillId, piPremiseId, bitemIds, svcIds);
        if (CollectionUtil.isNotEmpty(list1) && CollectionUtil.isNotEmpty(list2)) {
            list1.addAll(list2);
        } else if (CollectionUtil.isNotEmpty(list2)) {
            list1 = list2;
        }
        if (CollectionUtil.isNotEmpty(list1)) {
            list1.sort(Comparator.comparing(Zs880SetupSqlSSacobfBatchResponse::getGeoAddressId, Comparator.nullsLast(String::compareTo))
                    .thenComparing(Zs880SetupSqlSSacobfBatchResponse::getBeDt, Comparator.nullsLast(String::compareTo))
                    .thenComparing(Zs880SetupSqlSSacobfBatchResponse::getEDt, Comparator.nullsLast(String::compareTo))
                    .thenComparing(Zs880SetupSqlSSacobfBatchResponse::getValType, Comparator.nullsFirst(String::compareTo).reversed()));
        }
//        return list1;
         return billExtrMapper.zs880SetupSqlSSacobf(piBillId, piPremiseId, rateIds, bitemIds, svcIds);
    }

    public List<Zs880SetupSqlSSacobfBatchResponse> zs880SetupSqlSSacobf_improved(String piBillId,
                                                                         String geoAddressId,
                                                                         List<BitemCalcBdWithAttr> bitemCalcBdWithAttrList,
                                                                         List<BitemWithReadAndQty> bitemWithReadAndQtyList,
                                                                         List<RateWithDtlAndCrit> rateWithDtlAndCritList,
                                                                         List<FinTranWithAdj> finTranWithAdjList,
                                                                         List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                                                         List<SvcDtlContractValEntity> contractValEntityList) {
        List<Zs880SetupSqlSSacobfBatchResponse> retVal = new ArrayList<>();
        Map<String, SvcDtlWithTypeAndBal> premiseSvcDtls = new HashMap<>();
        Set<String> finTranBxBitemIds = new HashSet<>();
        Set<String> premiseBitemIds = new HashSet<>();

        Map<String, BitemWithReadAndQty> bitems = new HashMap<>();
        Map<String, SvcDtlWithTypeAndBal> svcs = new HashMap<>();
        Map<String, RateWithDtlAndCrit> effRates = new HashMap<>();
        for (RateWithDtlAndCrit rate : rateWithDtlAndCritList) {
            String key = rate.getBillParamCode() + "-" + rate.getCritAttrTypeCode() + "-" + rate.getCritAttrVal();
            if (!effRates.containsKey(key)) {
                effRates.put(key,rate);
            }
            RateWithDtlAndCrit curr = effRates.get(key);
            if (rate.getEffDate().after(curr.getEffDate())) {
                effRates.put(key, rate);
            }
        }
        for (SvcDtlWithTypeAndBal svcdtl : svcDtlWithTypeAndBalList) {
            if (geoAddressId.equals(svcdtl.getGeoAddressId())) {
                premiseSvcDtls.put(svcdtl.getSvcId(), svcdtl);
            }
            svcs.put(svcdtl.getSvcId(), svcdtl);
        }
        for (BitemWithReadAndQty read : bitemWithReadAndQtyList) {
            if (geoAddressId.equals(read.getGeoAddressId())) {
                premiseBitemIds.add(read.getBitemId());
            }
            bitems.put(read.getBitemId(), read);
        }
        for (FinTranWithAdj ft : finTranWithAdjList) {
            if ("BX".equals(ft.getFinTranTypeInd())) {
                finTranBxBitemIds.add(ft.getFinTranTypeId());
            }
        }
        DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
        for (BitemCalcBdWithAttr calcbd : bitemCalcBdWithAttrList) {
            if (or(finTranBxBitemIds.contains(calcbd.getBitemId()),
                !premiseBitemIds.contains(calcbd.getBitemId()))) {
                continue;
            }
            String rateKey = calcbd.getAttrVal() + "-" + calcbd.getCalcAttrTypeCode() + "-" +
                calcbd.getCalcAttrVal();
            RateWithDtlAndCrit rate = effRates.get(rateKey);
            if (rate == null) {
                continue;
            }
            Zs880SetupSqlSSacobfBatchResponse row = new Zs880SetupSqlSSacobfBatchResponse();
            row.setValType("S");
            row.setAttrTypeCode(calcbd.getAttrTypeCode() == null ? "" : calcbd.getAttrTypeCode());
            row.setChgRateVal(rate.getChgRateVal());
            row.setBillParamCode(rate.getAttrVal());
            row.setGeoAddressId(geoAddressId);
            BitemWithReadAndQty read = bitems.get(calcbd.getBitemId());
            SvcDtlWithTypeAndBal svc = svcs.get(read.getSvcId());
            row.setBeDt(df.format(read.getEndDate()));
            row.setEDt(df.format(read.getEndDate()));
            row.setHsicCode(svc.getHsicCode());
            retVal.add(row);
        }
        for (SvcDtlContractValEntity contractVal : contractValEntityList) {
            Zs880SetupSqlSSacobfBatchResponse row = new Zs880SetupSqlSSacobfBatchResponse();
            row.setValType("C");
            for (BitemCalcBdWithAttr calcbd : bitemCalcBdWithAttrList) {
                if (contractVal.getContractValCode().equals(calcbd.getAttrVal())) {
                    row.setAttrTypeCode(calcbd.getCalcAttrTypeCode() == null ? "" : calcbd.getCalcAttrTypeCode());
                    row.setChgRateVal(contractVal.getContractVal());
                    row.setBillParamCode(contractVal.getContractValCode());
                    row.setGeoAddressId(geoAddressId);
                    row.setBeDt(df.format(contractVal.getEndDate()));
                    row.setEDt(df.format(contractVal.getEndDate()));
                    row.setAttrTypeCode(calcbd.getCalcAttrTypeCode());
                    BitemWithReadAndQty read = bitems.get(calcbd.getBitemId());
                    SvcDtlWithTypeAndBal svc = svcs.get(read.getSvcId());
                    row.setHsicCode(svc.getHsicCode());
                    retVal.add(row);
                    break;
                }
            }

        }
        return retVal;
    }

    @Override
    public String zs960SetupSqlSSacoed(String piBillId, String piPremiseId, String piBfCd) {
        return billExtrMapper.zs960SetupSqlSSacoed(piBillId, piPremiseId, piBfCd);
    }

    @Override
    public String ma070AddBsegMessage(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piPremiseId,
                                      String piBillMsgCd, String piLanguageCd, String piFormattedVal, String piContractEndDt) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        List<String> v_msg_parm = new ArrayList<>();
        //调用SQ170_GET_MSGBILL.ZS310_SETUP_SQL_S_MSGBILL存储过程
        String v_msg_on_bill = zs310SetupSqlSMsgbill(piBillMsgCd, piLanguageCd);
        // 设置 v_msg_parm(1)
        v_msg_parm.add(piFormattedVal.substring(0, Math.min(254, piFormattedVal.length())));
        // 设置 v_msg_parm(2)
        String day = piContractEndDt.substring(8, 10);
        String month = piContractEndDt.substring(5, 7);
        String year = piContractEndDt.substring(0, 4);
        v_msg_parm.add(day + "/" + month + "/" + year);
        //调用PA100_SUB_PARM_VALUES存储过程
        String po_msg_on_bill = pa100SubParmValues(v_msg_on_bill, v_msg_parm);
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3500");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("80");
        gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
        gvBillExtractionHeaderBatch1.setSortKeyPreId(piPremiseId);
        gvGlobalVariableBatch1.setW_bitemmsgs_cnt(gvGlobalVariableBatch1.getW_bitemmsgs_cnt() + 1);
        //调用ct_detail_for_printer_key_3500函数
        String v_bp_extr_dtl = ctDetailForPrinterKey3500(gvGlobalVariableBatch1.getW_bitemmsgs_cnt(), po_msg_on_bill);
        //调用construct_bill_extr_line函数
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        return po_msg_on_bill;
    }

    @Override
    public String zs310SetupSqlSMsgbill(String piBillMsgCd, String piLanguageCd) {
        return billExtrMapper.zs310SetupSqlSMsgbill(piBillMsgCd, piLanguageCd);
    }

    // 水表读数
    public void mr040GetMtrRead(String piUserId, Date piProcessDttm, BillEntity piBillRow,
                                String piPremiseId, String piListType,
                                List<BitemWithReadAndQty> bitemWithReadAndQtyList,
                                List<Map<String, Object>> cofcList) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        Map<String, Object> effCofc = null;
        Date now = new Date();
        for (Map<String,Object> cofc : cofcList) {
            String getGeoAddressId = (String) cofc.get("geo_address_id");
            if (!getGeoAddressId.equals(piPremiseId)) {
                continue;
            }
            Date effDate = (Date) cofc.get("eff_date");
            if (effCofc == null) {
                effCofc = cofc;
            }
            Date maxDate = (Date) effCofc.get("eff_date");
            if (maxDate.before(effDate) && !effDate.after(now)) {
                effCofc = cofc;
            }
        }
        Set<String> meterIds = new HashSet<>();
        List<Zs450SetupSqlSMtrrecBatchResponse> zs450Responses = new ArrayList<>();
        for (BitemWithReadAndQty bitem : bitemWithReadAndQtyList) {
            if (bitem.getEndReading() == null) {
                bitem.setEndReading(BigDecimal.ZERO);
            }
            if (meterIds.contains(bitem.getSerialNo() + "-" + bitem.getEndReading().toPlainString())) {
                continue;
            }
            String cofcPremiseId = bitem.getGeoAddressId(); // (String) effCofc.get("geo_address_id");
            meterIds.add(bitem.getSerialNo() + "-" + bitem.getEndReading().toPlainString());
            Zs450SetupSqlSMtrrecBatchResponse res = new Zs450SetupSqlSMtrrecBatchResponse();
            res.setAccountId(piBillRow.getAccountId()/*(String)effCofc.get("account_id")*/);
            res.setMeterNo(bitem.getSerialNo());
            res.setSpId(bitem.getGeoAddressId());
            res.setStartReadDt(DateUtils.formatDateToString(bitem.getStartReadDate(), "dd/MM/yyyy"));
            res.setEndReadDt(DateUtils.formatDateToString(bitem.getEndReadDate(), "dd/MM/yyyy"));
            if (bitem.getStartReading() != null) {
                int val = bitem.getStartReading().setScale(0, RoundingMode.HALF_UP).intValue();
                res.setStartReading(val);
            } else {
                res.setStartReading(0);
            }
            if (bitem.getEndReading() != null) {
                int val = bitem.getEndReading().setScale(0, RoundingMode.HALF_UP).intValue();
                res.setEndReading(val);
            } else {
                res.setStartReading(0);
            }
            res.setStartReadFlg(bitem.getStartReadTypeInd());
            res.setEndReadFlg(bitem.getEndReadTypeInd());
            if (bitem.getFinalQty() != null) {
                res.setCsm(bitem.getFinalQty().doubleValue());
            } else {
                res.setCsm(0.0);
            }
            res.setUomCd(bitem.getUomCode());
            res.setUsageFlg(bitem.getCalcUsageInd());
            zs450Responses.add(res);
        }
        zs450Responses.sort((o1, o2) -> {
            DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
            try {
                Date sd1 = df.parse(o1.getStartReadDt());
                Date sd2 = df.parse(o2.getStartReadDt());
                int cmp = sd1.compareTo(sd2);
                if (cmp != 0) {
                    return cmp;
                }
            } catch (ParseException e) {
                return 0;
            }
            try {
                Date d1 = df.parse(o1.getEndReadDt());
                Date d2 = df.parse(o2.getEndReadDt());
                return d1.compareTo(d2);
            } catch (ParseException e) {
                return 0;
            }
        });
        //调用SQ360_GET_MTRREC.ZS450_SETUP_SQL_S_MTRREC存储过程
        // 性能调优，参考上面的代码
//        List<Zs450SetupSqlSMtrrecBatchResponse> zs450Responses2 = zs450SetupSqlSMtrrec(piBillRow.getBillId(), piPremiseId, piListType);
        Double w_tot_csm = 0.0;
        int w_mtrrec_cnt = 0;
        for (Zs450SetupSqlSMtrrecBatchResponse zs450Response : zs450Responses) {
            if (zs450Response.getSpId() == null) {
                continue;
            }
            String v_mtrrec_START_READ_FLG = zs450Response.getStartReadFlg() == null ? "" : zs450Response.getStartReadFlg();
            String v_mtrrec_END_READ_FLG = zs450Response.getEndReadFlg() == null ? "" : zs450Response.getEndReadFlg();
            String v_mtrrec_START_READ_DT = zs450Response.getStartReadDt() == null ? "" : zs450Response.getStartReadDt();
            String v_mtrrec_END_READ_DT = zs450Response.getEndReadDt() == null ? "" : zs450Response.getEndReadDt();
            String v_mtrrec_UOM_CD = zs450Response.getUomCd() == null ? "" : zs450Response.getUomCd();
            Double v_mtrrec_CSM = zs450Response.getCsm() == null ? 0.0 : zs450Response.getCsm();
            String v_mtrrec_METER_NO = zs450Response.getMeterNo() == null ? "" : zs450Response.getMeterNo();
            String v_mtrrec_USAGE_IND = zs450Response.getUsageFlg() == null ? "" : zs450Response.getUsageFlg();
            String v_mtrrec_PRE_ID = zs450Response.getSpId() == null ? "" : zs450Response.getSpId();
            //添加AccountId+校验位进行拼接
            String v_mtrrec_ACCOUNT_ID = zs450Response.getAccountId() == null ? "" : zs450Response.getAccountId();
            //获取accountId的校验位
            String checkDigit = VerifyUtils.getCheckDigit(v_mtrrec_ACCOUNT_ID);
            v_mtrrec_ACCOUNT_ID += checkDigit;
            Date v_mtrrec_END_READ_DTTM = zs450Response.getEndReadDttm();
            int v_mtrrec_START_READING = zs450Response.getStartReading();
            int v_mtrrec_END_READING = zs450Response.getEndReading();
            String v_start_read_type = null;
            String v_end_read_type = null;
            String w_uom_cd = null;
            Double w_csm = null;
            String w_METER_NO = null;

            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2400");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("50");
            w_mtrrec_cnt++;
            String sort_key_line_seq = String.format("%04d", w_mtrrec_cnt);
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(sort_key_line_seq);
            // 处理开始读取类型
            if ("50".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "S";
            } else if ("30".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "E";
            } else if ("35".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "E";
            } else if ("40".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "E";
            } else if ("80".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "F";
            } else if ("70".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "A";
            } else if ("60".equals(v_mtrrec_START_READ_FLG)) {
                v_start_read_type = "A";
            } else {
                v_start_read_type = "X";
            }

            // 处理结束读取类型
            if ("50".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "S";
            } else if ("30".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "E";
            } else if ("35".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "E";
            } else if ("40".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "E";
            } else if ("80".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "F";
            } else if ("70".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "A";
            } else if ("60".equals(v_mtrrec_END_READ_FLG)) {
                v_end_read_type = "A";
            } else {
                v_end_read_type = "A";
            }

            w_uom_cd = v_mtrrec_UOM_CD;
            if ("GAL".equals(v_mtrrec_UOM_CD)) {
                w_csm = v_mtrrec_CSM * 4.54609;
            } else {
                w_csm = v_mtrrec_CSM;
            }

            w_METER_NO = v_mtrrec_METER_NO;
            if ("-".equals(v_mtrrec_USAGE_IND.trim())) {
                //调用SQ130_GET_SECACCT.ZS120_SETUP_SQL_S_SECACCT存储过程
                String v_secacct_acct_id = zs120SetupSqlSSecacct(v_mtrrec_PRE_ID, v_mtrrec_END_READ_DTTM);
                if (v_secacct_acct_id != null && !v_secacct_acct_id.trim().isEmpty()) {
                    w_METER_NO = v_secacct_acct_id;
                } else {
                    w_METER_NO = v_mtrrec_METER_NO;
                }
                w_tot_csm += w_csm;

            } else {

            }
            if (v_mtrrec_START_READING > 999999999) {
                //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
                zz000SetupSqlICiMsg(piBillRow.getBillId(), "30", "v_mtrrec_START_READING>9(9):" + v_mtrrec_START_READING);
            }
            if (v_mtrrec_END_READING > 999999999) {
                //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
                zz000SetupSqlICiMsg(piBillRow.getBillId(), "30", "v_mtrrec_START_READING>9(9):" + v_mtrrec_END_READING);
            }
            //调用ct_detail_for_printer_key_2400函数获取v_bp_extr_dtl值
            v_mtrrec_ACCOUNT_ID = v_mtrrec_ACCOUNT_ID == null ? "" : v_mtrrec_ACCOUNT_ID;
            String v_bp_extr_dtl = ctDetailForPrinterKey2400(w_METER_NO, v_mtrrec_START_READ_DT, v_mtrrec_START_READING, v_start_read_type,
                    v_mtrrec_END_READ_DT, v_mtrrec_END_READING, v_end_read_type, v_mtrrec_USAGE_IND, trunc(w_csm, 6),
                    v_mtrrec_UOM_CD, trunc(w_tot_csm, 6),v_mtrrec_ACCOUNT_ID);
            //调用construct_bill_extr_line函数
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2400");
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        }

    }

    //截取小数点后decimalPlace位数
    private static Double trunc(Double d, int decimalPlace) {
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(decimalPlace, RoundingMode.DOWN);
        return bd.doubleValue();
    }

    public List<Zs450SetupSqlSMtrrecBatchResponse> zs450SetupSqlSMtrrec(String piBillId, String piPremiseId, String piListType) {
        return billExtrMapper.zs450SetupSqlSMtrrec(piBillId, piPremiseId, piListType);
    }

    @Override
    public String zs120SetupSqlSSecacct(String piPremiseId, Date piReadDttm) {
        return billExtrMapper.zs120SetupSqlSSecacct(piPremiseId, piReadDttm);
    }

    public void mj010GetList1Spbal(String piUserId, Date piProcessDttm,
                                   BillEntity piBillRow,
                                   String piBillType,
                                   String piListType,
                                   String piPrebillId,
                                   List<FinTranWithAdj> finTranWithAdjList,
                                   List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                   List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
                                   List<BillExtrSvcDtlGttEntity> billExtrSvcDtlGttEntityList,
                                   List<BitemWithReadAndQty> bitemList,
                                   String geoAddressId) {
        if (geoAddressId != null && geoAddressId.startsWith("00000000")) {
            geoAddressId = null;
        }
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        AlgorithmParameters params = algorithmParams.get();
        BigDecimal v_total_credit = BigDecimal.ZERO;
        BigDecimal v_total_debit = BigDecimal.ZERO;
        BigDecimal v_deposit_offset = BigDecimal.ZERO;
        BigDecimal v_charges = BigDecimal.ZERO;
        BigDecimal v_odd_cents = BigDecimal.ZERO;
        BigDecimal v_balance_bf = BigDecimal.ZERO;
        BigDecimal v_amount_due = BigDecimal.ZERO;
        BigDecimal v_balance_cf = BigDecimal.ZERO;
        String v_bp_extr_dtl = "";
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3000");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("60");
        if ("01".equals(piBillType)) {
            //调用SQ620_GET_SPBAL1.ZS730_SETUP_SQL_S_SPBAL1存储过程
//            Zs730SetupSqlSSpbal1BatchResponse zs730Response = zs730SetupSqlSSpbal1(
//                piPrebillId, piBillRow.getBillId(), params.getDepositAdjCd(), "1PM");
            Zs730SetupSqlSSpbal1BatchResponse zs730Response = zs730SetupSqlSSpbal1(
                finTranWithAdjList,
                svcDtlWithTypeAndBalList,
                prevSvcDtlWithTypeAndBalList,
                billExtrSvcDtlGttEntityList,
                bitemList,
                params.getDepositAdjCd(), "1PM", geoAddressId);
            v_total_credit = zs730Response.getPoTotalCredit();
            v_total_debit = zs730Response.getPoTotalDebit();
            v_deposit_offset = zs730Response.getPoDepositOffset();
            v_charges = zs730Response.getPoCharges();
            v_odd_cents = zs730Response.getPoOddCents();
            v_balance_bf = v_total_debit.add(v_total_credit);
            if ("Y".equals(gvGlobalVariableBatch1.getW_min_amt_due_s())) {
                v_balance_cf = v_charges.multiply(BigDecimal.valueOf(-1));
                v_amount_due = BigDecimal.ZERO;
            } else {
                v_balance_cf = v_odd_cents.multiply(BigDecimal.valueOf(-1));
                v_amount_due = v_charges.subtract(v_odd_cents);
            }
            // 判断是不是final bill
            if (checkFinalBill(piBillRow.getBillId()) && v_deposit_offset.compareTo(BigDecimal.ZERO) > 0) {
                v_charges = v_balance_cf.add(v_deposit_offset).negate();
            }
            if (checkFinalBill(piBillRow.getBillId()) && v_deposit_offset.compareTo(BigDecimal.ZERO) < 0) {
                v_charges = v_balance_cf.add(v_deposit_offset).negate();
            }
            //调用ct_detail_for_printer_key_3000函数获取v_bp_extr_dtl值
            v_bp_extr_dtl = ctDetailForPrinterKey3000(v_balance_bf,
                    v_charges, v_deposit_offset, v_balance_cf, v_amount_due);
        } else {
            //pi_bill_type 02 OPEN-ITEM
            //调用SQ630_GET_OSPBAL.ZS460_SETUP_SQL_S_OSPBAL存储过程
            Zs460SetupSqlSOspbalBatchResponse zs460Response = zs460SetupSqlSOspbal(piBillRow.getBillId(), "1PM");
            v_charges = zs460Response.getPoCharges();
            v_odd_cents = zs460Response.getPoOddCents();
            //调用ct_detail_for_printer_key_3000函数获取v_bp_extr_dtl值
            v_bp_extr_dtl = ctDetailForPrinterKey3000(BigDecimal.ZERO, v_charges, BigDecimal.ZERO,
                    v_odd_cents.multiply(BigDecimal.valueOf(-1)),
                    v_charges.subtract(v_odd_cents));
        }
        //调用construct_bill_extr_line函数
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
    }

    @Override
    public Zs730SetupSqlSSpbal1BatchResponse zs730SetupSqlSSpbal1(String piPrebillId, String piBillId, String piAdjTypeCd, String piListType) {
        return billExtrMapper.zs730SetupSqlSSpbal1(piPrebillId, piBillId, piAdjTypeCd, piListType);
    }

    public Zs730SetupSqlSSpbal1BatchResponse zs730SetupSqlSSpbal1(List<FinTranWithAdj> finTranWithAdjList,
                                                                  List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                                                  List<SvcDtlWithTypeAndBal> prevSvcDtlWithTypeAndBalList,
                                                                  List<BillExtrSvcDtlGttEntity> billExtrSvcDtlGttEntityList,
                                                                  List<BitemWithReadAndQty> bitemList,
                                                                  String piAdjTypeCd, String piListType, String geoAddressId) {
        Zs730SetupSqlSSpbal1BatchResponse retVal = new Zs730SetupSqlSSpbal1BatchResponse();
        BigDecimal charges = BigDecimal.ZERO;
        BigDecimal oddCents = BigDecimal.ZERO;
        BigDecimal depositOffset = BigDecimal.ZERO;
        BigDecimal totalDebit = BigDecimal.ZERO;
        BigDecimal totalCredit = BigDecimal.ZERO;
        boolean existingAdjForAdjTypeCd = false;
        boolean notExistingBitem = true;
        Set<String> svcIdsOfListType = new HashSet<>();
        // 区分premise的svc
        Set<String> premisedSvcIds = new HashSet<>();
        for (BillExtrSvcDtlGttEntity gtt : billExtrSvcDtlGttEntityList) {
            svcIdsOfListType.add(gtt.getSvcId());
        }
        for (BitemWithReadAndQty bitem : bitemList) {
            for (FinTranWithAdj ft : finTranWithAdjList) {
                if (bitem.getBitemId().equals(ft.getFinTranTypeId())) {
                    notExistingBitem = false;
                    break;
                }
            }
        }
        for (FinTranWithAdj adj : finTranWithAdjList) {
            if (piAdjTypeCd.equals(adj.getAdjTypeCode())) {
                existingAdjForAdjTypeCd = true;
                break;
            }
        }
        // charges
        for (SvcDtlWithTypeAndBal bal : svcDtlWithTypeAndBalList) {
            if (!premiseEquals(geoAddressId, bal.getGeoAddressId())) {
                continue;
            }
            if (!svcIdsOfListType.isEmpty() && !svcIdsOfListType.contains(bal.getSvcId())) {
                continue;
            }

            premisedSvcIds.add(bal.getSvcId());
            charges = charges.add(bal.getCurAmt());
            BigDecimal shifted = bal.getCurAmt().multiply(BigDecimal.valueOf(100));
            BigInteger intPart = shifted.setScale(0, RoundingMode.DOWN).toBigInteger();
            oddCents = oddCents.add(new BigDecimal("0.0" + intPart.mod(BigInteger.TEN).intValue()));
        }

        // deposit offset
        for (FinTranWithAdj ft : finTranWithAdjList) {
            if (!premisedSvcIds.contains(ft.getSvcId())) {
                continue;
            }
            if (!and(
                svcIdsOfListType.contains(ft.getSvcId()),
                "Y".equals(ft.getFreezeSw()),
                in(ft.getFinTranTypeInd(), "AD", "AX"))) {
                continue;
            }
            if (in(ft.getAdjTypeCode(), "WO SYNC", "SYNC")) {
                continue;
            }
            depositOffset = depositOffset.add(ft.getCurAmt());
        }
        // total debit
        for (SvcDtlWithTypeAndBal bal : prevSvcDtlWithTypeAndBalList) {
            if (!premiseEquals(geoAddressId, bal.getGeoAddressId())) {
                continue;
            }
            if (!svcIdsOfListType.contains(bal.getSvcId())) {
                continue;
            }
            totalDebit = totalDebit.add(bal.getCurAmt());
        }
        // total credit
        for (FinTranWithAdj ft : finTranWithAdjList) {
            if (!premisedSvcIds.contains(ft.getSvcId())) {
                continue;
            }
            if (!and(
                svcIdsOfListType.contains(ft.getSvcId()),
                "Y".equals(ft.getFreezeSw()),
                !piAdjTypeCd.equals(ft.getRelatedId()),
                or(in(ft.getFinTranTypeInd(), "PS", "PX"),
                    and(in(ft.getFinTranTypeInd(), "AD", "AX"),
                    "N".equals(ft.getShowOnBillSw()), !existingAdjForAdjTypeCd),
                    and("BX".equals(ft.getFinTranTypeInd()), "N".equals(ft.getShowOnBillSw()), notExistingBitem)))) {
                continue;
            }
            totalCredit = totalCredit.add(ft.getCurAmt());
        }
        retVal.setPoTotalCredit(totalCredit);
        retVal.setPoTotalDebit(totalDebit);
        retVal.setPoCharges(charges);
        retVal.setPoOddCents(oddCents);
        retVal.setPoDepositOffset(depositOffset);
        return retVal;
    }

    @Override
    public Zs460SetupSqlSOspbalBatchResponse zs460SetupSqlSOspbal(String piBillId, String piListType) {
        Zs460SetupSqlSOspbalBatchResponse retVal =  billExtrMapper.zs460SetupSqlSOspbal(piBillId, piListType);
        if (retVal == null) {
            retVal = new Zs460SetupSqlSOspbalBatchResponse();
            retVal.setPoCharges(BigDecimal.ZERO);
            retVal.setPoOddCents(BigDecimal.ZERO);
        } else {
            // Open Item账户零头恒为零
            retVal.setPoOddCents(BigDecimal.ZERO);
        }
        return retVal;
    }

    @Override
    public void mg010CanBillMessage(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piPremiseId, String piLanguageCd, String piListType) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        AlgorithmParameters params = algorithmParams.get();
        String v_canbitem_descr = null;
        String v_canbitem_internal_flg = null;
        String v_canbitem_can_rsn_cd = null;
        String W_WITH_CRCAN_SW = "N";
        String W_WITH_CANBSEG_SW = "N";
        String v_msg_on_bill = null;
        String v_bill_msg_cd = null;
        List<String> v_msg_parm = new ArrayList<>();
        //调用SQ680_GET_S_CANBSEG.ZS790_SETUP_SQL_S_CANBSEG存储过程
        List<Zs790SetupSqlSCanbsegBatchResponse> zs790Responses = zs790SetupSqlSCanbseg(piBillRow.getBillId(), params.getCanExcludeList(),
                piLanguageCd, piListType);
        for (Zs790SetupSqlSCanbsegBatchResponse zs790Response : zs790Responses) {
            v_canbitem_descr = zs790Response.getDescr();
            v_canbitem_internal_flg = zs790Response.getInternalFlg();
            v_canbitem_can_rsn_cd = zs790Response.getCanRsnCd();
            if ("N".equals(W_WITH_CRCAN_SW) && "N".equals(v_canbitem_internal_flg)) {
                //调用SQ170_GET_MSGBILL.ZS310_SETUP_SQL_S_MSGBILL存储过程
                v_msg_on_bill = zs310SetupSqlSMsgbill(params.getCanbillMsgCd2(), piLanguageCd);
                gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3500");
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("80");
                gvBillExtractionHeaderBatch1.setSortKeyPreId(piPremiseId);
                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
                gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
                gvGlobalVariableBatch1.setW_bitemmsgs_cnt(gvGlobalVariableBatch1.getW_bitemmsgs_cnt() + 1);
                //调用ct_detail_for_printer_key_3500函数获取v_bp_extr_dtl值
                String v_bp_extr_dtl = ctDetailForPrinterKey3500(gvGlobalVariableBatch1.getW_bitemmsgs_cnt(), v_msg_on_bill);
                //调用construct_bill_extr_line函数
                String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
                mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
                W_WITH_CRCAN_SW = "Y";
            }
            if ("N".equals(W_WITH_CANBSEG_SW)) {
                if (!"CDD5".equals(v_canbitem_can_rsn_cd)) {
                    v_bill_msg_cd = params.getCanbillMsgCd();
                } else {
                    v_bill_msg_cd = "CRB4";
                }
                //调用SQ170_GET_MSGBILL.ZS310_SETUP_SQL_S_MSGBILL存储过程
                v_msg_on_bill = zs310SetupSqlSMsgbill(v_bill_msg_cd, piLanguageCd);
                W_WITH_CANBSEG_SW = "Y";
                // 开始转换逻辑
                String msgParam = v_canbitem_descr.substring(0, Math.min(254, v_canbitem_descr.length()));
                if (msgParam.contains("*")) {
                    msgParam = msgParam.replace("*", "");
                }
                v_msg_parm.add(0, msgParam);
                //调用PA100_SUB_PARM_VALUES存储过程，获取v_sub_msg_on_bill值
                String v_sub_msg_on_bill = pa100SubParmValues(v_msg_on_bill, v_msg_parm);
                gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3500");
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("80");
                gvBillExtractionHeaderBatch1.setSortKeyPreId(piPremiseId);
                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
                gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
                gvGlobalVariableBatch1.setW_bitemmsgs_cnt(gvGlobalVariableBatch1.getW_bitemmsgs_cnt() + 1);
                //调用ct_detail_for_printer_key_3500函数获取v_bp_extr_dtl值
                String v_bp_extr_dtl = ctDetailForPrinterKey3500(gvGlobalVariableBatch1.getW_bitemmsgs_cnt(), v_sub_msg_on_bill);
                //调用construct_bill_extr_line函数
                String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
                mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
            }
        }

    }

    @Override
    public List<Zs790SetupSqlSCanbsegBatchResponse> zs790SetupSqlSCanbseg(String piBillId, String piFieldName, String piLanguageCd, String piListType) {
        List<String> bitemIds = billExtrMapper.zs790SetupSqlSCanbseg_01(piBillId);
        if (CollectionUtil.isEmpty(bitemIds)) {
            return new ArrayList<>();
        }
        List<String> svcIds = billExtrMapper.zs790SetupSqlSCanbseg_02(piBillId, piListType);
        if (CollectionUtil.isEmpty(svcIds)) {
            return new ArrayList<>();
        }
        List<String> cancRsnCodes = billExtrMapper.zs790SetupSqlSCanbseg_03(piFieldName);
        List<Zs790SetupSqlSCanbsegBatchResponse> list = billExtrMapper.zs790SetupSqlSCanbseg(piLanguageCd, bitemIds, svcIds, cancRsnCodes);
        if (CollectionUtil.isNotEmpty(list)) {
            list.sort(Comparator.comparing(Zs790SetupSqlSCanbsegBatchResponse::getInternalFlg,Comparator.nullsLast(String::compareTo))
                    .thenComparing(Zs790SetupSqlSCanbsegBatchResponse::getDescr,Comparator.nullsLast(String::compareTo)));
        }
        return list;
    }

    public void mo010EndSp(String piUserId, Date piProcessDttm, BillEntity piBillRow) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3999");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("90");
        //调用ct_detail_for_printer_key_3999函数获取v_bp_extr_dtl值
        String v_bp_extr_dtl = "";
        //调用construct_bill_extr_line函数
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
    }

    public Mq020GetList2BatchResponse mq020GetList2(Mc010PremiseGroupBatchRequest request,
                                                    List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                                    List<FinTranWithAdj> finTranWithAdjList) {
        Mq020GetList2BatchResponse response = new Mq020GetList2BatchResponse();
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        int v_list2_cnt = 0;
        List<Zs670SetupSqlSList2BatchDto> v_list2 = new ArrayList<>();
        String v_temp_pre_id = null;
        String v_temp_svc_id = null;
        int po_dummy_pre_cnt = 0;
        String v_check_point;
        String v_bp_extr_dtl;
        String v_bp_extr_lines;
        //调用SQ560_GET_LIST2.ZS670_SETUP_SQL_S_LIST2存储过程
        List<Zs670SetupSqlSList2BatchResponse> zs670Responses = zs670SetupSqlSList2(
                request.getPiBillId(), request.getPiLanguageCd(), params.getInstFldName(),
                params.getDisputeFldName(), params.getDepositAdjCd(), params.getOverpaySdType(), "1SD",
                svcDtlWithTypeAndBalList, finTranWithAdjList);
        for (Zs670SetupSqlSList2BatchResponse zs670Response : zs670Responses) {
            if (v_list2_cnt >= gvGlobalVariableBatch1.getW_max_cnt()) {
                break;
            }
            //调用XQ050_INSERT_SA_LIST.ZS941_SETUP_SQL_I_TEMPSA存储过程插入数据
            try {
                billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(zs670Response.getSvcId(), "2SD"));
            } catch (Throwable cause) {

            }
            v_list2_cnt++;
            Zs670SetupSqlSList2BatchDto vList2Data = new Zs670SetupSqlSList2BatchDto();
            vList2Data.setSvcId(zs670Response.getSvcId());
            vList2Data.setSpecialRoleFlg(zs670Response.getSpecialRoleInd());
            vList2Data.setDfltDescrOnBill(zs670Response.getSvcTypeDfltDescOnBill());
            vList2Data.setPreId(null);
            if ("BONLROUT".equals(request.getPiBillRoutingRow().getBatchJobCode())) {
                vList2Data.setBillPrtPrioFlg(String.format("%04d", v_list2_cnt).substring(0, 2));
            } else {
                vList2Data.setBillPrtPrioFlg(String.format("%02d", v_list2_cnt).substring(0, 2));
            }
            vList2Data.setSubTotalSw("N");
            v_list2.add(vList2Data);
        }
        for (int w_idx1 = 1; w_idx1 <= v_list2.size(); w_idx1++) {
            if (po_dummy_pre_cnt == 0) {
                gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2000");
                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("50");
                po_dummy_pre_cnt++;
                gvBillExtractionHeaderBatch1.setSortKeyPerId(String.valueOf(po_dummy_pre_cnt));
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("10");
                v_temp_pre_id = String.valueOf(po_dummy_pre_cnt);
                v_list2.get(w_idx1 - 1).setPreId(v_temp_pre_id);
                v_temp_svc_id = v_list2.get(w_idx1 - 1).getSvcId();
                //调用ctDetailForPrinterKey2000方法
                v_bp_extr_dtl = ctDetailForPrinterKey2000(String.valueOf(po_dummy_pre_cnt), "N", " ", " ",
                        " ", " ", " ");
                //调用construct_bill_extr_line函数
                v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
                mx010WriteExtractData(request.getPiBillRow().getBillId(), v_bp_extr_lines, request.getPiUserId(), request.getPiProcessDttm());
                //调用MJ020_GET_LIST2_SPBAL存储过程
                gvBillExtractionHeaderBatch1.setSortKeyPreId(v_temp_pre_id);
                gvBillExtractionHeaderBatch1.setSortKeySvcId(v_temp_svc_id);
                gvBillExtractionHeaderBatch1.setSortKeyPerId(request.getPiBillRoutingRow().getPersonId());
                mj020GetList2Spbal(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(), "2SD",
                        request.getPiPrebillId(), request.getPiBillType());

                //调用MG010_CAN_BILL_MESSAGE存储过程
                mg010CanBillMessage(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow(), v_temp_pre_id,
                        request.getPiLanguageCd(), "2SD");
                //调用MO010_END_SP存储过程
                mo010EndSp(request.getPiUserId(), request.getPiProcessDttm(), request.getPiBillRow());
            }
            v_temp_svc_id = v_list2.get(w_idx1 - 1).getSvcId();
            v_list2.get(w_idx1 - 1).setPrePrintPriority("00");
            v_list2.get(w_idx1 - 1).setPreId(v_temp_pre_id);
        }
        response.setPoList2(v_list2);
        response.setPoDummyPreCnt(po_dummy_pre_cnt);
        return response;
    }

    public List<Zs670SetupSqlSList2BatchResponse> zs670SetupSqlSList2(
            String piBillId, String piLanguageCd, String piFieldName1, String piFieldName2,
            String piAdjTypeCd, String piSvcTypeCd, String piListType,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<FinTranWithAdj> finTranWithAdjList) {
//        List<Zs670SetupSqlSList2BatchResponse> retVal = new ArrayList<>();
//        for (SvcDtlWithTypeAndBal svcDtl : svcDtlWithTypeAndBalList) {
//            Integer count = billExtrSvcDtlGttMapper.selectCount(new QueryWrapper<BillExtrSvcDtlGttEntity>().lambda()
//                    .eq(BillExtrSvcDtlGttEntity::getSvcId, svcDtl.getSvcId())
//                    .eq(BillExtrSvcDtlGttEntity::getListType, piListType));
//            if (count > 0) {
//                continue;
//            }
//            if (piSvcTypeCd.equals(svcDtl.getSvcTypeCode()) && svcDtl.getCurAmt().compareTo(BigDecimal.ZERO) == 0) {
//                continue;
//            }
//            boolean isMatchingFinTran = true;
//            for (FinTranWithAdj finTran : finTranWithAdjList) {
//                if (!finTran.getSvcId().equals(svcDtl.getSvcId())) {
//                    isMatchingFinTran = false;
//                    break;
//                }
//                if (!"Y".equals(finTran.getFreezeSw())) {
//                    isMatchingFinTran = false;
//                    break;
//                }
//                if (!"Y".equals(finTran.getShowOnBillSw())) {
//                    isMatchingFinTran = false;
//                    break;
//                }
//                if (!"BS".equals(finTran.getFinTranTypeInd()) &&
//                    !"BX".equals(finTran.getFinTranTypeInd()) &&
//                    !"AD".equals(finTran.getFinTranTypeInd()) &&
//                    !"AX".equals(finTran.getFinTranTypeInd())) {
//                    isMatchingFinTran = false;
//                    break;
//                }
//                if ("AD".equals(finTran.getFinTranTypeInd()) ||
//                    "AX".equals(finTran.getFinTranTypeInd())) {
//                    if (piAdjTypeCd.equals(finTran.getAdjTypeCode())) {
//                        isMatchingFinTran = false;
//                        break;
//                    }
//                }
//            }
//            if (isMatchingFinTran) {
//                Zs670SetupSqlSList2BatchResponse res = new Zs670SetupSqlSList2BatchResponse();
//                res.setSvcId(svcDtl.getSvcId());
//                res.setBillPrintPrio(svcDtl.getBillPrintPrio());
//                res.setSpecialRoleInd(svcDtl.getSpecialRoleInd());
//                if ("ENG".equals(piLanguageCd)) {
//                    res.setSvcTypeDfltDescOnBill(svcDtl.getSvcTypeDfltDescOnBill());
//                } else {
//                    res.setSvcTypeDfltDescOnBill(svcDtl.getSvcTypeDfltDescOnBillTc());
//                }
//                retVal.add(res);
//            }
//            // TODO
////            AND SD.SVC_TYPE_CODE NOT IN (
////                    SELECT VAL
////                    FROM CFG_LKUP_CODE
////                    WHERE LKUP_TYPE = #{piFieldName1}
////            OR LKUP_TYPE = #{piFieldName2}
//
//        }
//        return retVal;
        return billExtrMapper.zs670SetupSqlSList2(piBillId, piLanguageCd, piFieldName1, piFieldName2, piAdjTypeCd, piSvcTypeCd, piListType);
    }

    @Override
    public void mj020GetList2Spbal(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piListType, String piPrebillId, String piBillType) {
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        BigDecimal v_total_credit = BigDecimal.ZERO;
        BigDecimal v_total_debit = BigDecimal.ZERO;
        BigDecimal v_deposit_offset = BigDecimal.ZERO;
        BigDecimal v_charges = BigDecimal.ZERO;
        BigDecimal v_odd_cents = BigDecimal.ZERO;
        BigDecimal v_balance_bf = BigDecimal.ZERO;
        BigDecimal v_balance_cf = BigDecimal.ZERO;
        BigDecimal v_amount_due = BigDecimal.ZERO;
        String v_bp_extr_dtl;
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3000");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("60");
        if ("01".equals(piBillType)) {
            //调用SQ640_GET_SPBAL2.ZS750_SETUP_SQL_S_SPBAL2存储过程
            Zs730SetupSqlSSpbal1BatchResponse zs730Response = zs750SetupSqlSSpbal2(piPrebillId, piBillRow.getBillId(), params.getDepositAdjCd(), "2SD");
            v_total_credit = zs730Response.getPoTotalCredit();
            v_total_debit = zs730Response.getPoTotalDebit();
            v_deposit_offset = zs730Response.getPoDepositOffset();
            v_charges = zs730Response.getPoCharges();
            v_odd_cents = zs730Response.getPoOddCents();
            v_balance_bf = v_total_debit.add(v_total_credit);
            if ("Y".equals(gvGlobalVariableBatch1.getW_min_amt_due_s())) {
                v_balance_cf = v_charges.multiply(BigDecimal.valueOf(-1));
                v_amount_due = BigDecimal.ZERO;
            } else {
                v_balance_cf = v_odd_cents.multiply(BigDecimal.valueOf(-1));
                v_amount_due = v_charges.subtract(v_odd_cents);
            }
            //调用ct_detail_for_printer_key_3000函数获取v_bp_extr_dtl值
            v_bp_extr_dtl = ctDetailForPrinterKey3000(v_balance_bf,
                    v_charges.subtract(v_deposit_offset),
                    v_deposit_offset, v_balance_cf, v_amount_due);
        } else {
            //pi_bill_type 02 OPEN-ITEM
            //调用SQ630_GET_OSPBAL.ZS460_SETUP_SQL_S_OSPBAL存储过程
            Zs460SetupSqlSOspbalBatchResponse zs460Response = zs460SetupSqlSOspbal(piBillRow.getBillId(), "2SD");
            v_charges = zs460Response.getPoCharges();
            v_odd_cents = zs460Response.getPoOddCents();
            //调用ct_detail_for_printer_key_3000函数获取v_bp_extr_dtl值
            v_bp_extr_dtl = ctDetailForPrinterKey3000(BigDecimal.ZERO, v_charges, BigDecimal.ZERO,
                    v_odd_cents.multiply(BigDecimal.valueOf(-1)),
                    v_charges.subtract(v_odd_cents));
        }
        //调用construct_bill_extr_line函数
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
    }

    @Override
    public Zs730SetupSqlSSpbal1BatchResponse zs750SetupSqlSSpbal2(String piPrebillId, String piBillId, String piAdjTypeCd, String piListType) {
        return billExtrMapper.zs750SetupSqlSSpbal2(piPrebillId, piBillId, piAdjTypeCd, piListType);
    }

    public void mq030GetList3(String piUserId, Date piProcessDttm, BillEntity piBillRow, BillRoutingBatch piBillRoutingRow,
                              String piLanguageCd, String piBillType, String piPrebillId, int piDummyPreCnt,
                              List<SvcDtlWithTypeAndBal> svcList3) {

        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        int v_dummy_pre_cnt = piDummyPreCnt;
        int v_list3_cnt = 0;
        String v_temp_pre_id = null;
        String v_temp_svc_id = null;
        String vAddress1 = "";
        String vAddress2 = "";
        String vAddress3 = "";
        String vAddress4 = "";
        String v_prem_sw = null;
        String v_serial_nbr = "";
        List<Zs680SetupSqlSList3BatchDto> v_list3 = new ArrayList<>();
        //调用SQ570_GET_LIST3.ZS680_SETUP_SQL_S_LIST3存储过程
        List<Zs680SetupSqlSList3BatchResponse> zs680Responses = zs680SetupSqlSList3(piBillRow.getBillId(),
            piLanguageCd, params.getInstFldName(), params.getDisputeFldName(),
            "1SD", "2SD");
        for (Zs680SetupSqlSList3BatchResponse zs680Response : zs680Responses) {
            if (v_list3_cnt > gvGlobalVariableBatch1.getW_max_cnt()) {
                break;
            }
//            list3_out_data.pre_id = cursorData.pre_id;
//            list3_out_data.svc_id = cursorData.svc_id;
//            list3_out_data.bill_prt_prio_flg = cursorData.bill_prt_prio_flg;
//            list3_out_data.special_role_flg = cursorData.special_role_flg;
//            list3_out_data.dflt_descr_on_bill = cursorData.dflt_descr_on_bill;
            //调用XQ050_INSERT_SA_LIST.ZS941_SETUP_SQL_I_TEMPSA存储过程插入临时表
            //zs941SetupSqlITempsa("3SD", zs680Response.getSvcId());
            try {
                billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(zs680Response.getSvcId(), "3SD"));
            } catch (Throwable cause) {

            }
            v_list3_cnt = v_list3_cnt + 1;
            Zs680SetupSqlSList3BatchDto vList3Data = new Zs680SetupSqlSList3BatchDto();
            vList3Data.setPreId(zs680Response.getGeoAddressId());
            vList3Data.setSvcId(zs680Response.getSvcId());
            vList3Data.setSpecialRoleFlg(String.format("%04d", v_list3_cnt).substring(0, 2));
            vList3Data.setDfltDescrOnBill(zs680Response.getSvcTypeDfltDescOnBill());
            if ("BONLROUT".equals(piBillRoutingRow.getBatchJobCode())) {
                vList3Data.setBillPrtPrioFlg(String.format("%04d", v_list3_cnt).substring(0, 2));
            } else {
                vList3Data.setBillPrtPrioFlg(String.format("%02d", v_list3_cnt).substring(0, 2));
            }
            vList3Data.setSubTotalSw("N");
            v_list3.add(vList3Data);
        }
        String w_with_dum3_sw = "N";
        for (int w_idx1 = 0; w_idx1 < v_list3_cnt; w_idx1++) {
            if ((!v_list3.get(w_idx1).getPreId().equals(v_temp_pre_id) || v_temp_pre_id == null) && "N".equals(w_with_dum3_sw)) {
                if (!v_list3.get(w_idx1).getPreId().equals("1")) {
                    //调用XQ051_DELETE_SA_LIST.ZS951_SETUP_SQL_D_TEMPSA存储过程删除临时表
//                    xq051DeleteSAList.delete("3PM");
//                    billExtrSvcDtlGttMapper.delete(Wrappers.<BillExtrSvcDtlGttEntity>lambdaQuery()
//                            .eq(BillExtrSvcDtlGttEntity::getListType, "3PM"));
                    int v_list3_sd_list_cnt = 0;
                    for (int w_idx2 = 0; w_idx2 < v_list3.size(); w_idx2++) {
                        if (v_list3.get(w_idx1).getPreId().equals(v_list3.get(w_idx2).getPreId())) {
                            v_list3_sd_list_cnt++;
//                            VList3SDListData vList3SDListDataPre = new VList3SDListData();
//                            vList3SDListDataPre.svc_id = v_list3.get(w_idx2).svc_id;
//                            v_list3_sd_list_pre.add(vList3SDListDataPre);
                            //调用XQ050_INSERT_SA_LIST.ZS941_SETUP_SQL_I_TEMPSA存储过程插入临时表
                            zs941SetupSqlITempsa("3PM", v_list3.get(w_idx2).getSvcId());
                            // FIXME: 此处存在 duplicate Key的错误，待解决
                            try {
                                billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(v_list3.get(w_idx2).getSvcId(), "3PM"));
                            } catch (Throwable cause) {
                                // TRACER.error(cause.getMessage(), cause);
                            }

                        }
                    }
                    v_temp_pre_id = v_list3.get(w_idx1).getPreId();
                    v_temp_svc_id = v_list3.get(w_idx1).getSvcId();
                    gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2000");
                    if ("ZHT".equals(piLanguageCd) || "CHI".equals(piLanguageCd) && "Y".equals(params.getChiAddress())) {
                        //调用SQ490_GET_ALTPREM.ZS580_SETUP_SQL_S_ALTPREM存储过程获取PremiseZht表地址信息
                        String v_alt_prem_found_sw = "N";
                        PremiseZhtEntity premiseZhtEntity = premiseZhtMapper.selectById(v_temp_pre_id);
                        if (premiseZhtEntity != null) {
                            v_alt_prem_found_sw = "Y";
                            vAddress1 = premiseZhtEntity.getAddrLn1();
                            vAddress2 = premiseZhtEntity.getAddrLn2();
                            vAddress3 = premiseZhtEntity.getAddrLn3();
                            vAddress4 = premiseZhtEntity.getAddrLn4();
                        }
                        if ("N".equals(v_alt_prem_found_sw)) {
                            //调用SQ240_GET_SPPREM.ZS330_SETUP_SQL_S_SPPREM存储过程获取PREMISE表的地址信息
                            GeoAddressntity geoAddressntity = premiseMapper.selectById(v_temp_pre_id);
                            vAddress1 = geoAddressntity.getAddrLn1();
                            vAddress2 = geoAddressntity.getAddrLn2();
                            vAddress3 = geoAddressntity.getAddrLn3();
                            vAddress4 = geoAddressntity.getAddrLn4();
                        }
                    } else {
                        //调用SQ240_GET_SPPREM.ZS330_SETUP_SQL_S_SPPREM存储过程PREMISE表地址信息
                        GeoAddressntity geoAddressntity = premiseMapper.selectById(v_temp_pre_id);
                        vAddress1 = geoAddressntity.getAddrLn1();
                        vAddress2 = geoAddressntity.getAddrLn2();
                        vAddress3 = geoAddressntity.getAddrLn3();
                        vAddress4 = geoAddressntity.getAddrLn4();
                    }
                    //调用SQ140_GET_MTRNBR.ZS130_SETUP_SQL_S_MTRNBR存储过程
                    v_serial_nbr = zs130SetupSqlSMtrnbr(v_temp_pre_id, piBillRow.getCmpltDt(), v_temp_svc_id);
                    v_prem_sw = "Y";
                } else {
                    if ("N".equals(w_with_dum3_sw)) {
//                        billExtrSvcDtlGttMapper.delete(Wrappers.<BillExtrSvcDtlGttEntity>lambdaQuery()
//                                .eq(BillExtrSvcDtlGttEntity::getListType, "3PM"));
                        v_dummy_pre_cnt = v_dummy_pre_cnt + 1;
                        v_temp_pre_id = String.format("%010d", v_dummy_pre_cnt);
//                        v_list3_sd_list_cnt=0;
                        for (int w_idx2 = 0; w_idx2 < v_list3.size(); w_idx2++) {
                            if (v_list3.get(w_idx1).getPreId().equals(v_list3.get(w_idx2).getPreId())) {
                                //调用XQ050_INSERT_SA_LIST.ZS941_SETUP_SQL_I_TEMPSA存储过程插入临时表
                                zs941SetupSqlITempsa("3PM", v_list3.get(w_idx2).getSvcId());
                                try {
                                    billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(v_list3.get(w_idx2).getSvcId(), "3PM"));
                                } catch (Throwable cause) {

                                }

                            }
                        }
                        w_with_dum3_sw = "Y";
                        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2000");
                        v_prem_sw = "N";
                    }
                }
                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
                gvBillExtractionHeaderBatch1.setSortKeyPreId(v_temp_pre_id);
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("10");
                //调用ctDetailForPrinterKey2000方法
                String v_bp_extr_dtl = ctDetailForPrinterKey2000(v_temp_pre_id, v_prem_sw, vAddress1, vAddress2,
                        vAddress3, vAddress4, v_serial_nbr);
                //调用construct_bill_extr_line函数
                String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
                mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
                //调用MJ030_GET_LIST3_SPBAL存储过程
                mj030GetList3Spbal(piUserId, piProcessDttm, piBillRow, "3PM", piPrebillId, piBillType, svcList3);
                //调用MO010_END_SP存储过程
                mo010EndSp(piUserId, piProcessDttm, piBillRow);
            }
        }
    }

    @Override
    public List<Zs680SetupSqlSList3BatchResponse> zs680SetupSqlSList3(String piBillId, String piLanguageCd, String piFieldName1,
                                                                      String piFieldName2, String piListType1, String piListType2) {
        return billExtrMapper.zs680SetupSqlSList3(piBillId, piLanguageCd, piFieldName1, piFieldName2, piListType1, piListType2);
    }

    @Override
    public void zs941SetupSqlITempsa(String piListType, String piSvcId) {
        try {
            billExtrSvcDtlGttMapper.insert(new BillExtrSvcDtlGttEntity(piListType, piSvcId));
        } catch (Throwable cause) {

        }
//        billExtrMapper.zs941SetupSqlITempsa(piListType, piSvcId);
    }

    public void mj030GetList3Spbal(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piListType, String piPrebillId,
                                   String piBillType, List<SvcDtlWithTypeAndBal> svcList3) {
        Set<String> svcIds = new HashSet<>();
        svcList3.forEach(svc -> {
            svcIds.add(svc.getSvcId());
        });
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        BigDecimal v_bal_bf = BigDecimal.ZERO;
        BigDecimal v_total_ft = BigDecimal.ZERO;
        BigDecimal v_deposit_offset = BigDecimal.ZERO;
        BigDecimal v_charges = BigDecimal.ZERO;
        BigDecimal v_odd_cents = BigDecimal.ZERO;
        BigDecimal v_balance_bf = BigDecimal.ZERO;
        BigDecimal v_balance_cf = BigDecimal.ZERO;
        BigDecimal v_amount_due = BigDecimal.ZERO;
        String v_bp_extr_dtl = "";
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3000");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("60");
        if ("01".equals(piBillType)) {
            //调用SQ650_GET_SPBAL3.ZS760_SETUP_SQL_S_SPBAL3存储过程
            Zs760SetupSqlSSpbal3BatchResponse zs760Response = zs760SetupSqlSSpbal3(piPrebillId, piBillRow.getBillId(),
                "XFER-DPF"/* params.getDepositAdjCd() */, "3PM" /*UNUSED*/, svcIds);
            v_bal_bf = zs760Response.getPoBalBf();
            v_total_ft = zs760Response.getPoTotalFt();
            v_deposit_offset = zs760Response.getPoDepositOffset();
            v_charges = zs760Response.getPoCharges();
            v_odd_cents = zs760Response.getPoOddCents();
            v_balance_bf = v_bal_bf.add(v_total_ft);
            if ("Y".equals(gvGlobalVariableBatch1.getW_min_amt_due_s())) {
//                v_balance_bf = v_charges;
                v_balance_cf = v_charges.multiply(BigDecimal.valueOf(-1));
                v_amount_due = BigDecimal.ZERO;
            } else {
                v_balance_cf = v_odd_cents.multiply(BigDecimal.valueOf(-1));
                v_amount_due = v_charges.subtract(v_odd_cents);
            }
            //调用ct_detail_for_printer_key_3000函数获取v_bp_extr_dtl值
            v_bp_extr_dtl = ctDetailForPrinterKey3000(v_balance_bf, v_charges, v_deposit_offset, v_balance_cf, v_amount_due);
        } else {
            //调用SQ650_GET_SPBAL3.ZS760_SETUP_SQL_S_SPBAL3存储过程
            Zs760SetupSqlSSpbal3BatchResponse zs760Response = zs760SetupSqlSSpbal3(piPrebillId, piBillRow.getBillId(),
                params.getDepositAdjCd(), "3PM", svcIds);
            v_bal_bf = zs760Response.getPoBalBf();
            v_total_ft = zs760Response.getPoTotalFt();
            v_deposit_offset = zs760Response.getPoDepositOffset();
            v_charges = zs760Response.getPoCharges();
            v_odd_cents = zs760Response.getPoOddCents();
            v_balance_bf = v_bal_bf.add(v_total_ft);
            //调用ct_detail_for_printer_key_3000函数获取v_bp_extr_dtl值
            v_bp_extr_dtl = ctDetailForPrinterKey3000(
                    v_balance_bf, v_charges, v_deposit_offset, v_balance_cf, v_amount_due);
        }
        //调用construct_bill_extr_line函数
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
    }

    public Zs760SetupSqlSSpbal3BatchResponse zs760SetupSqlSSpbal3(String piPrebillId, String piBillId, String piAdjTypeCd, String piListType, Set<String> svcIds) {
        return billExtrMapper.zs760SetupSqlSSpbal3(piPrebillId, piBillId, piAdjTypeCd, piListType, svcIds);
    }

    public void me010CreateRecord3500(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piLanguageCd,
                                      List<Mq010GetList1BtachResponse> piList1,
                                      List<Zs670SetupSqlSList2BatchDto> piList2,
                                      List<String> piDfbitemCd,
                                      List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
                                      List<BitemWithReadAndQty> bitemWithReadAndQtyList,
                                      List<BitemMsgWithMsg> bitemMsgWithMsgList,
                                      List<FinTranWithAdj> finTranWithAdjList) {
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        List<String> v_dfbitem_cd = new ArrayList<>();
        String v_msg_on_bill = null;
        String v_bill_msg_cd = null;
        String v_svc_id = null;
        String v_extr_msg_on_bill = null;
        double v_total_bfw_amt = 0;
        double v_subsidy_realize = 0;
        double v_total_cfw_amt = 0;
        double v_total_adj_amt = 0;
        for (int i = 1; i <= 30; i++) {
            if (i <= piDfbitemCd.size()) {
                v_dfbitem_cd.add(piDfbitemCd.get(i - 1));
            } else {
                v_dfbitem_cd.add("    ");
            }
        }
        //调用SQ250_GET_BSEGMSG.ZS340_SETUP_SQL_S_BSEGMSG存储过程
        List<Zs340SetupSqlSBsegmsgBatchResponse> zs340Responses = zs340SetupSqlSBsegmsg(piBillRow.getBillId(), piLanguageCd,
                params.getSurchgBitemCd(), "AUTO", v_dfbitem_cd,
                bitemWithReadAndQtyList, bitemMsgWithMsgList, finTranWithAdjList);
        for (Zs340SetupSqlSBsegmsgBatchResponse zs340Response : zs340Responses) {
            v_msg_on_bill = zs340Response.getMsgOnBill();
            v_bill_msg_cd = zs340Response.getBillMsgCode();
            v_svc_id = zs340Response.getSvcId();
            String v_sd_found_in_list1_sw = "N";
            String v_sd_found_in_list2_sw = "N";
            for (Mq010GetList1BtachResponse list1 : piList1) {
                if (v_svc_id != null && v_svc_id.equals(list1.getSvcId())) {
                    gvBillExtractionHeaderBatch1.setSortKeyPreId(list1.getPreId());
                    gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(list1.getPrePrintPriority());
                    gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
                    v_sd_found_in_list1_sw = "Y";
                }
            }
            Set<String> premiseIds = new HashSet<>();
            Set<String> bxBitemIds = new HashSet<>();
            for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
                if (svc.getSvcId().equals(v_svc_id) && svc.getGeoAddressId() != null) {
                    premiseIds.add(svc.getGeoAddressId());
                }
            }
            for (FinTranWithAdj fin : finTranWithAdjList) {
                if ("BX".equals(fin.getFinTranTypeInd())) {
                    bxBitemIds.add(fin.getFinTranTypeId());
                }
            }
            if ("N".equals(v_sd_found_in_list1_sw)) {
                for (Zs670SetupSqlSList2BatchDto list2 : piList2) {
                    if ("Y".equals(v_sd_found_in_list2_sw)) {
                        break;
                    }
                    if (list2.getSvcId().equals(v_svc_id)) {
                        gvBillExtractionHeaderBatch1.setSortKeyPerId(list2.getPreId());
                        gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(list2.getPrePrintPriority());
                        gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
                        v_sd_found_in_list2_sw = "Y";
                    }
                }
            }
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3500");
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("80");
            gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(" ");
            gvBillExtractionHeaderBatch1.setSortKeyBitemId(" ");
            gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(0);
            gvBillExtractionHeaderBatch1.setSortKeyBitemGrp(" ");
            gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(" ");
            gvGlobalVariableBatch1.setW_bitemmsgs_cnt(gvGlobalVariableBatch1.getW_bitemmsgs_cnt() + 1);

            if ("ADC4".equals(v_bill_msg_cd)) {
                Date startDate = java.sql.Date.valueOf("2999-12-31");
                Date endDate = java.sql.Date.valueOf("1970-01-01");
                BigDecimal adcVal = BigDecimal.ZERO;
                Map<String, BitemWithReadAndQty> premiseQtyList = new HashMap<>();
                List<Zs440SetupSqlSCsmrecV2BatchResponse> zs440Responses = zs440SetupSqlSCsmrec(
                    piBillRow.getBillId(), bitemWithReadAndQtyList, finTranWithAdjList);
                BigDecimal avg = BigDecimal.ZERO;
                if (!zs440Responses.isEmpty()) {
                    avg = BigDecimal.valueOf(zs440Responses.get(0).getAvgCsm()).multiply(BigDecimal.valueOf(1000L)).setScale(0, RoundingMode.HALF_UP);
                }
                v_extr_msg_on_bill = zs103SetupSqlSMsgRepl(v_msg_on_bill, avg.toPlainString());
            } else if ("ADC1".equals(v_bill_msg_cd)) {
                Date startDate = java.sql.Date.valueOf("2999-12-31");
                Date endDate = java.sql.Date.valueOf("1970-01-01");
                BigDecimal adcVal = BigDecimal.ZERO;
                Map<String, BitemWithReadAndQty> premiseQtyList = new HashMap<>();
                for (BitemWithReadAndQty bitem : bitemWithReadAndQtyList) {
                    if (bitem.getStartDate().before(startDate)) {
                        startDate = bitem.getStartDate();
                    }
                    if (bitem.getEndDate().after(endDate)) {
                        endDate = bitem.getEndDate();
                    }
                    if (!premiseQtyList.containsKey(bitem.getGeoAddressId())  && bitem.getFinalQty() != null) {
                        adcVal = adcVal.add(bitem.getFinalQty());
                        premiseQtyList.put(bitem.getGeoAddressId(), bitem);
                    }
                }
                long days = (endDate.getTime() - startDate.getTime()) / (60 * 60 * 24 * 1000);
                BigDecimal avg = adcVal.divide(BigDecimal.valueOf(days), 3, RoundingMode.HALF_UP);
//                if (StringUtil.isNotEmpty(v_svc_id.trim()) && !premiseIds.isEmpty()) {
//                    //调用函数PA400_GET_ADC_C_VAL.ZS101_SETUP_SQL_S_GET_SP_ADC_C存储过程
//                    Integer v_cnt = zs101SetupSqlSGetSpAdcC(piBillRow.getBillId(), v_svc_id);
//                    if (v_cnt >= 1) {
//                        gvGlobalVariableBatch1.setW_adc_sw("Y");
//                        double v_adc_val = 0;
//                        //调用PA411_GET_ADC1_VAL.ZS112_SETUP_SQL_S_GET_SP_ADC1
//                        v_adc_val = zs112SetupSqlSGetSpAdc1(piBillRow.getBillId(), v_svc_id);
//                        //调用PA402_MSG_REPL_ADC.ZS103_SETUP_SQL_S_MSG_REPL存储过程
                        v_extr_msg_on_bill = zs103SetupSqlSMsgRepl(v_msg_on_bill, avg.toPlainString()/*new DecimalFormat("000000000.000").format(v_adc_val)*/);
//                    }
//                }
            } else if ("SBD1".equals(v_bill_msg_cd) || "SBD3".equals(v_bill_msg_cd)) {
                //调用PA412_GET_SBDDTL.ZS113_SETUP_SQL_S_SBDDTL存储过程
                Zs113SetupSqlSSbddtlBatchResponse zs113Response = zs113SetupSqlSSbddtl(piBillRow.getBillId());
                if (zs113Response != null) {
                    v_total_bfw_amt = zs113Response.getPoTotalBfwAmt();
                    v_subsidy_realize = zs113Response.getPoSubsidyRealize();
                    v_total_cfw_amt = zs113Response.getPoTotalCfwAmt();
                    v_total_adj_amt = zs113Response.getPoTotalAdjAmt();
                    //调用PA403_MSG_REPL_SBD.ZS104_SETUP_SQL_S_SBDM_REPL存储过程
                    v_extr_msg_on_bill = zs104SetupSqlSSbdmRepl(v_msg_on_bill, new DecimalFormat("000000000.000").format(v_total_bfw_amt),
                        new DecimalFormat("000000000.000").format(v_subsidy_realize),
                        new DecimalFormat("000000000.000").format(v_total_cfw_amt),
                        new DecimalFormat("000000000.000").format(v_total_adj_amt));//new DecimalFormat("000000000.000").format(v_adc_val)
                }
            } else {
                v_extr_msg_on_bill = v_msg_on_bill;
            }
            //调用ct_detail_for_printer_key_3500函数
            String v_bp_extr_dtl = ctDetailForPrinterKey3500(gvGlobalVariableBatch1.getW_bitemmsgs_cnt(), v_extr_msg_on_bill);
            //调用construct_bill_extr_line函数
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        }

    }

    public List<Zs340SetupSqlSBsegmsgBatchResponse> zs340SetupSqlSBsegmsg(
            String piBillId, String piLanguageCd, String piSurchgBitemCd,
            String piApayBitemCd, List<String> piBitemCd,
            List<BitemWithReadAndQty> bitemWithReadAndQtyList,
            List<BitemMsgWithMsg> bitemMsgWithMsgList,
            List<FinTranWithAdj> finTranWithAdjList) {
        List<Zs340SetupSqlSBsegmsgBatchResponse> retVal = new ArrayList<>();
        Set<String> piBitemCds = new HashSet<>(piBitemCd);
        for (BitemMsgWithMsg msg : bitemMsgWithMsgList) {
            if (msg.getBillMsgCode().equals(piSurchgBitemCd)) {
                continue;
            }
            if (msg.getBillMsgCode().equals(piApayBitemCd)) {
                continue;
            }
            if (piBitemCds.contains(msg.getBillMsgCode())) {
                continue;
            }

            BitemWithReadAndQty found = null;
            for (BitemWithReadAndQty bitem : bitemWithReadAndQtyList) {
                if (!bitem.getBitemId().equals(msg.getBitemId())) {
                    continue;
                }
                if (bitem.getCalcUsageInd() != null && !"+".equals(bitem.getCalcUsageInd())) {
                    continue;
                }
                BitemWithReadAndQty bitemBx = null;
                for (FinTranWithAdj finTran : finTranWithAdjList) {
                    if (finTran.getFinTranTypeId().equals(bitem.getBitemId()) &&
                        finTran.getFinTranTypeInd().equals("BX")) {
                        bitemBx = bitem;
                        break;
                    }
                }
                if (bitemBx != null) {
                    continue;
                }
                found = bitem;
            }
            if (found != null) {
                Zs340SetupSqlSBsegmsgBatchResponse res = new Zs340SetupSqlSBsegmsgBatchResponse();
                res.setSvcId(found.getSvcId());
                if ("ENG".equals(piLanguageCd)) {
                    res.setMsgOnBill(msg.getMsgOnBill());
                } else {
                    res.setMsgOnBill(msg.getMsgOnBillTc());
                }
                res.setBillMsgCode(msg.getBillMsgCode());
                retVal.add(res);
            }
        }
        return retVal;
        // 性能优化
//        return billExtrMapper.zs340SetupSqlSBsegmsg(piBillId, piLanguageCd, piSurchgBitemCd, piApayBitemCd, piBitemCd);
    }

    @Override
    public Integer zs101SetupSqlSGetSpAdcC(String piBillId, String piSvcId) {
        return billExtrMapper.zs101SetupSqlSGetSpAdcC(piBillId, piSvcId);
    }

    @Override
    public Integer zs102SetupSqlSGetSpAdc(String piBillId, String piSvcId) {
        return billExtrMapper.zs102SetupSqlSGetSpAdc(piBillId, piSvcId);
    }

    @Override
    public Integer zs112SetupSqlSGetSpAdc1(String piBillId, String piSvcId) {
        return billExtrMapper.zs112SetupSqlSGetSpAdc1(piBillId, piSvcId);
    }

    @Override
    public Zs113SetupSqlSSbddtlBatchResponse zs113SetupSqlSSbddtl(String piBillId) {
        return billExtrMapper.zs113SetupSqlSSbddtl(piBillId);
    }

    /*
     * 工商業用水收费记录
     *
     * ================================
     * 【2018-12-13 ~ 2019-04-10】
     * BX 512191367232 非住宅食水供水 - 工商業用途
     * BX 512191381975 工商業用水 - 排污費
     *
     * 【2018-12-13 ~ 2019-04-15】
     * BS 512191310680 工商業用水 - 排污費
     * BS 512191370762 非住宅食水供水 - 工商業用途
     *
     * ================================
     * 【2019-04-11 ~ 2019-08-09】
     * BX 512191355153 非住宅食水供水 - 工商業用途
     * BX 512191333350 工商業用水 - 排污費
     *
     * 【2019-04-16 ~ 2019-08-16】
     * BS 512191314585 工商業用水 - 排污費
     * BS 512191331707 非住宅食水供水 - 工商業用途
     *
     * ================================
     * 【2019-08-10 ~ 2020-12-07】
     * BX 512191307835 工商業用水 - 排污費
     * BX 512191372318 非住宅食水供水 - 工商業用途
     *
     * ================================
     * 【2019-08-17 ~ 2019-12-10】
     * BS 512191322912 工商業用水 - 排污費
     * BS 512191336028 非住宅食水供水 - 工商業用途
     *
     * 【2019-12-11 ~ 2020-04-08】
     * BS 512191355313 工商業用水 - 排污費
     * BS 512191313572 非住宅食水供水 - 工商業用途
     *
     * 【2020-04-09 ~ 2020-08-10】
     * BS 512191359407 工商業用水 - 排污費
     * BS 512191349208 非住宅食水供水 - 工商業用途
     *
     * 【2020-08-11 ~ 2021-01-06】
     * BS 512191330086 非住宅食水供水 - 工商業用途
     * BS 512191366824 工商業用水 - 排污費
     *
     * ================================
     * 【一次性收费】
     * 2020-11-30
     * BS 512191351863 遺失水錶的收費
     * BS 512191351863 水錶遺失
     *
     * 2021-03-29
     * BS 512191338146 重新接駁消防或內部供水系統的收費
     *
     * ================================
     * 说明：
     * BX = 主账单记录
     * BS = 服务费用或附加收费
     *
     * 周期收费通常包含：
     *   - 非住宅食水供水（工商業用途）
     *   - 工商業用水 - 排污費
     *
     * 另含一次性收费项目（如水表遗失、重新接驳费用）
     */
    public Map<String, Object> me020CreateRecord3200(
            String piUserId, Date piProcessDttm, BillEntity piBillRow, String piLanguageCd,
            List<Mq010GetList1BtachResponse> piList1, List<Zs670SetupSqlSList2BatchDto> piList2,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList, List<BitemWithReadAndQty> bitemList,
            List<FinTranWithAdj> finTranWithAdjList) {
        Map<String, Object> resultMap = new HashMap<>();
        AlgorithmParameters params = algorithmParams.get();
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();

        String W_TEMP_SVC_ID = null;
        String W_WITH_CANCAHR_SW = "N";
        int W_TEMP_BITEM_CALCHDR_SEQ = 0;
        String v_start_dt_vc = null;
        String v_end_dt_vc = null;
        String W_SPECIAL_ROLE_FLG = null;
        String W_TEMP_BITEM_ID = null;
        //调用SQ690_GET_BSEGCH.ZS800_SETUP_SQL_S_BSEGCH存储过程
        List<Zs800SetupSqlSBsegchBatchResponse> zs800Responses = zs800SetupSqlSBsegch(
                piBillRow.getBillId(), svcDtlWithTypeAndBalList, bitemList, finTranWithAdjList);

        List<Zs800SetupSqlSBsegchBatchResponse> revisedBillItems = new ArrayList<>();
        for (Zs800SetupSqlSBsegchBatchResponse billItem : zs800Responses) {
            if (!billItem.getBitemId().equals("000000000000")) {
                revisedBillItems.add(billItem);
            }
        }
        revisedBillItems.sort((a, b) -> {
            int cmp = a.getSvcId().compareTo(b.getSvcId());
            if (cmp != 0) {
                return cmp;
            }
            // 同一个svc下对日期排序
            return a.getStartDt().compareTo(b.getStartDt());
        });
        zs800Responses.sort((a, b) -> {
            int cmp = a.getSvcId().compareTo(b.getSvcId());
            if (cmp != 0) {
                return cmp;
            }
            // 同一个svc下对日期排序
            cmp = a.getBitemId().compareTo(b.getBitemId());
            if (cmp != 0) {
                return cmp;
            }
            return a.getStartDt().compareTo(b.getStartDt());
        });
        for (int i = 0; i < revisedBillItems.size(); i++) {
            Zs800SetupSqlSBsegchBatchResponse revisedItem = revisedBillItems.get(i);
            for (Zs800SetupSqlSBsegchBatchResponse originalItem : zs800Responses) {
                if (!originalItem.getBitemId().equals("000000000000")) {
                    continue;
                }
                if (!originalItem.getSvcId().equals(revisedItem.getSvcId())) {
                    continue;
                }
                if (!originalItem.getStartDt().before(revisedItem.getStartDt()) &&
                    !originalItem.getEndDt().after(revisedItem.getEndDt())) {
                    revisedBillItems.add(i, originalItem);
                    i++;
                }
            }
        }
        zs800Responses = revisedBillItems;
        // 重新排序，注意配对
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            for (Zs800SetupSqlSBsegchBatchResponse zs800Response : zs800Responses) {
                if (zs800Response.getSvcId().equals(svc.getSvcId())) {
                    zs800Response.setSvcTypeCode(svc.getSvcTypeCode());
                }
            }
        }
        Integer v_final_bill_cnt = billMsgMapper.selectCount(new QueryWrapper<BillMsgEntity>().lambda()
            .eq(BillMsgEntity::getBillId, piBillRow.getBillId())
            .eq(BillMsgEntity::getBillMsgCode, "FNB1"));
        for (Zs800SetupSqlSBsegchBatchResponse zs800Response : zs800Responses) {
            if (zs800Response.getSvcTypeCode() != null && zs800Response.getSvcTypeCode().endsWith("-D")) {
                zs800Response.setPricntDtSw("N");
                // 最终账单的水费按金必须显示日期
                if (zs800Response.getSvcTypeCode().startsWith("W-") &&
                    zs800Response.getSvcTypeCode().endsWith("-D") &&
                    v_final_bill_cnt != 0) {
                    zs800Response.setPricntDtSw("Y");
                }
            }
            if (zs800Response.getPricntDtSw() == null) {
                zs800Response.setPricntDtSw("Y");
            }
            // 格式化日期
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            v_start_dt_vc = sdf.format(zs800Response.getStartDt());
            v_end_dt_vc = sdf.format(zs800Response.getEndDt());
            String v_sd_found_in_list1_sw = "N";
            String v_sd_found_in_list2_sw = "N";
            String v_descr_on_bill = null;
            for (Mq010GetList1BtachResponse item : piList1) {
                if ("Y".equals(v_sd_found_in_list1_sw)) {
                    break;
                }
                if (zs800Response.getSvcId().equals(item.getSvcId())) {
                    gvBillExtractionHeaderBatch1.setSortKeyPreId(item.getPreId());
                    gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item.getPrePrintPriority());
                    gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item.getBillPrtPrioFlg());
                    W_SPECIAL_ROLE_FLG = item.getSpecialRoleFlg();
                    if ("BC".equals(W_SPECIAL_ROLE_FLG)) {
                        //调用SQ730_GET_BSEGCH_DESC.ZS840_SETUP_SQL_S_BSCHDESC存储过程--获取表BITEM_CALC的DESC_ON_BILL信息，RATE_CODE is null
                        v_descr_on_bill = zs840SetupSqlSBschdesc(zs800Response.getBitemId(), zs800Response.getEndDt());
//                        SQ730_GET_BSEGCH_DESC.execute(bitemchOutData.getBitem_id(), bitemchOutData.getEnd_dt(), v_descr_on_bill);
                    } else {
                        v_descr_on_bill = null;
                    }
                    v_sd_found_in_list1_sw = "Y";
                    item.setSubTotalSw("Y");
                    break;
                }

            }
            if ("N".equals(v_sd_found_in_list1_sw)) {
                for (Zs670SetupSqlSList2BatchDto item : piList2) {
                    if ("Y".equals(v_sd_found_in_list2_sw)) {
                        break;
                    }
                    if (zs800Response.getSvcId().equals(item.getSvcId())) {
                        gvBillExtractionHeaderBatch1.setSortKeyPreId(item.getPreId());
                        gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item.getPrePrintPriority());
                        gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item.getBillPrtPrioFlg());
                        W_SPECIAL_ROLE_FLG = item.getSpecialRoleFlg();
                        if ("BC".equals(W_SPECIAL_ROLE_FLG)) {
                            //调用SQ730_GET_BSEGCH_DESC.ZS840_SETUP_SQL_S_BSCHDESC存储过程--获取表BITEM_CALC的DESC_ON_BILL信息，RATE_CODE is null
                            v_descr_on_bill = zs840SetupSqlSBschdesc(zs800Response.getBitemId(), zs800Response.getEndDt());
//                        SQ730_GET_BSEGCH_DESC.execute(bitemchOutData.getBitem_id(), bitemchOutData.getEnd_dt(), v_descr_on_bill);
                        } else {
                            v_descr_on_bill = null;
                        }
                        v_sd_found_in_list2_sw = "Y";
                        item.setSubTotalSw("Y");
                        break;
                    }
                }
            }
            if ("000000000000".equals(zs800Response.getBitemId())) {
                W_WITH_CANCAHR_SW = "Y";
                if (StringUtil.isEmpty(gvGlobalVariableBatch1.getW_can_descr())) {
                    //调用SQ040_GET_CANDESC.ZS110_SETUP_SQL_S_CANDESC存储过程
                    // 原先开列项和现有开列项
                    Zs110SetupSqlSCandescBatchResponse zs110Response = zs110SetupSqlSCandesc(piLanguageCd);
                    gvGlobalVariableBatch1.setW_can_descr(zs110Response.getPoCanDescr());
                    gvGlobalVariableBatch1.setW_reb_descr(zs110Response.getPoRebDescr());
                }
                v_descr_on_bill = gvGlobalVariableBatch1.getW_can_descr();
                if ("CD".equals(W_SPECIAL_ROLE_FLG)) {
//                    v_start_dt_vc = null;
//                    v_end_dt_vc = null;
                }
            } else {
                if ("Y".equals(W_WITH_CANCAHR_SW)) {
                    v_descr_on_bill = gvGlobalVariableBatch1.getW_reb_descr();
                    if ("CD".equals(W_SPECIAL_ROLE_FLG)) {
//                        v_start_dt_vc = null;
//                        v_end_dt_vc = null;
                    }
                }
                if (!zs800Response.getSvcId().equals(W_TEMP_SVC_ID) && StringUtil.isNotEmpty(W_TEMP_SVC_ID)) {
                    W_WITH_CANCAHR_SW = "N";
                    if (v_descr_on_bill != null && v_descr_on_bill.equals(gvGlobalVariableBatch1.getW_reb_descr())) {
                        v_descr_on_bill = null;
                    }
                }
            }
            W_TEMP_BITEM_CALCHDR_SEQ += 1;
            if (("原先開列款額".equals(v_descr_on_bill) || "現已調整款額".equals(v_descr_on_bill) ||
                "Original billed amount".equals(v_descr_on_bill) ||
                "Revised billing amount".equals(v_descr_on_bill)) &&
                zs800Response.getSvcTypeCode().startsWith("W-") &&
                zs800Response.getSvcTypeCode().endsWith("-D") &&
                v_final_bill_cnt != 0) {
                zs800Response.setPricntDtSw("Y");
            }
            //调用ct_detail_for_printer_key_3200函数
            String v_bp_extr_dtl = ctDetailForPrinterKey3200(zs800Response.getSvcId(), zs800Response.getBitemId(),
                    W_TEMP_BITEM_CALCHDR_SEQ, v_start_dt_vc, v_end_dt_vc,
                    zs800Response.getPricntDtSw(), v_descr_on_bill,
                    zs800Response.getBitemCalcAmt());
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3200");
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("70");
            gvBillExtractionHeaderBatch1.setSortKeySdGrp("20");
            gvBillExtractionHeaderBatch1.setSortKeyBitemGrp("10");
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(" ");
            gvBillExtractionHeaderBatch1.setSortKeySvcId(zs800Response.getSvcId());
            W_TEMP_SVC_ID = zs800Response.getSvcId();
            gvBillExtractionHeaderBatch1.setSortKeyBitemId(zs800Response.getBitemId());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(formatter.format(zs800Response.getEndDt()));
            gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(W_TEMP_BITEM_CALCHDR_SEQ);
            //调用construct_bill_extr_line函数
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
            W_TEMP_BITEM_ID = zs800Response.getBitemId();
        }
        gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemId(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(0);
        gvBillExtractionHeaderBatch1.setSortKeyBitemGrp(" ");
        resultMap.put("po_list1", piList1);
        resultMap.put("po_list2", piList2);
        return resultMap;
    }

    public List<Zs800SetupSqlSBsegchBatchResponse> zs800SetupSqlSBsegch(
            String piBillId,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<BitemWithReadAndQty> bitemList,
            List<FinTranWithAdj> finTranWithAdjList) {
        List<Zs800SetupSqlSBsegchBatchResponse> retVal = new ArrayList<>();
        Set<String> added = new HashSet<>();
        Set<String> finTranTypeIds = new HashSet<>();
        for (FinTranWithAdj ft : finTranWithAdjList) {
            finTranTypeIds.add(ft.getFinTranTypeId());
        }
        if (finTranTypeIds.isEmpty()) {
            finTranTypeIds.add("0");
        }
        Set<String> notPrintDateBitems = new HashSet<>();
        List<Zs800SetupSqlSBsegchBatchResponse> res = billExtrMapper.zs800SetupSqlSBsegch(piBillId, finTranTypeIds);
        // 由于没去除subquery4，如果前面的数据及存在subquery4的，就直接置为N
        for (Zs800SetupSqlSBsegchBatchResponse row : res) {
            String key = row.getSvcId() + "#" + row.getBitemId() + "#" +
                row.getStartDt() + "#" + row.getEndDt();
            if ("N".equals(row.getPricntDtSw())) {
                notPrintDateBitems.add(key);
            }
        }
        for (Zs800SetupSqlSBsegchBatchResponse row : res) {
            String key = row.getSvcId() + "#" + row.getBitemId() + "#" +
                row.getStartDt() + "#" + row.getEndDt();
            if (notPrintDateBitems.contains(key)) {
                row.setPricntDtSw("N");
            }
        }
        res.sort((o1, o2) -> {
            if ("Y".equals(o1.getPricntDtSw())) {
                return -1;
            }
            if ("Y".equals(o2.getPricntDtSw())) {
                return 1;
            }
            return 0;
        });
        for (Zs800SetupSqlSBsegchBatchResponse row : res) {
            String key = row.getSvcId() + "#" + row.getBitemId() + "#" +
                row.getStartDt() + "#" + row.getEndDt();
            if (!added.contains(key)) {
                retVal.add(row);
                added.add(key);
            }
            for (Zs800SetupSqlSBsegchBatchResponse innerRow : retVal) {
                if (row.getSvcId().equals(innerRow.getSvcId())) {
                    if ("Y".equals(row.getPricntDtSw())) {
                        innerRow.setPricntDtSw("Y");
                    }
                }
            }
        }
        return retVal;
    }

    private void accumlateZs800(List<Zs800SetupSqlSBsegchBatchResponse> items, Zs800SetupSqlSBsegchBatchResponse item, boolean five) {
        boolean appended = false;
        for (Zs800SetupSqlSBsegchBatchResponse row : items) {
            if (!row.getSvcId().equals(item.getSvcId()) ||
                !row.getBitemId().equals(item.getBitemId()) ||
                !row.getStartDt().equals(item.getStartDt()) ||
                !row.getEndDt().equals(item.getEndDt())) {
                continue;
            }
            if (five) {
                if (!row.getBitemStsInd().equals(item.getBitemStsInd())) {
                    continue;
                }
            }
            row.setBitemCalcAmt(row.getBitemCalcAmt().add(item.getBitemCalcAmt()));
        }
        if (!appended) {
            items.add(item);
        }
    }

    @Override
    public String zs840SetupSqlSBschdesc(String piBitemId, Date piEndDt) {
        return billExtrMapper.zs840SetupSqlSBschdesc(piBitemId, piEndDt);
    }

    @Override
    public Zs110SetupSqlSCandescBatchResponse zs110SetupSqlSCandesc(String piLanguageCd) {
        return billExtrMapper.zs110SetupSqlSCandesc(piLanguageCd);
    }

    /**
     * 3210 水费包含明细（阶梯水费等）。
     */
    public Map<String, Object> me030CreateRecord3210(String piUserId, Date piProcessDttm, BillEntity piBillRow, String piLanguageCd,
                                                     List<Mq010GetList1BtachResponse> piList1,
                                                     List<Zs670SetupSqlSList2BatchDto> piList2,
                                                     String supplyNature) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        AlgorithmParameters params = algorithmParams.get();
        Map<String, Object> resultMap = new HashMap<>();
        String W_TEMP_BITEM_ID = null;
        String W_TEMP_BITEM_END_DT = null;
        double v_calc_amt = 0;
        supplyNature = supplyNature == null ? "" : supplyNature.toLowerCase();
        //调用SQ700_GET_BSEGCL.ZS810_SETUP_SQL_S_BSEGCL存储过程
        List<Zs810SetupSqlSBsegclBatchResponse> zs810Responses = zs810SetupSqlSBsegcl(
                piBillRow.getBillId(), "BPI-BSEG",
                (supplyNature.toLowerCase().contains("domestic") || supplyNature.contains("住宅供水")) ? 1 : 0);
        int W_TEMP_BITEM_CALCLN_SEQ1 = 0;
        int W_TEMP_BITEM_CALCHDR_SEQ = 0;
        for (Zs810SetupSqlSBsegclBatchResponse zs810Response : zs810Responses) {
            String svcId = zs810Response.getSvcId();
            String descrOnBill = zs810Response.getDescrOnBill();
            String v_sd_found_in_list1_sw = "N";
            String v_sd_found_in_list2_sw = "N";
            for (Mq010GetList1BtachResponse item : piList1) {
                if ("Y".equals(v_sd_found_in_list1_sw)) {
                    break;
                }
                if (svcId.equals(item.getSvcId())) {
                    gvBillExtractionHeaderBatch1.setSortKeyPreId(item.getPreId());
                    gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item.getPrePrintPriority());
                    gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item.getBillPrtPrioFlg());
                    v_sd_found_in_list1_sw = "Y";
                    item.setSubTotalSw("Y");
                    String W_WORD = descrOnBill;
                    String W_OUTWORD = descrOnBill;
                    if ("ZHT".equals(piLanguageCd) || "CHI".equals(piLanguageCd)) {
                        //调用OA030_SUBSTITUTE_UNIT存储过程
                        // 模拟 rtrim 函数，去除右侧空格
                        String trimmedDescr = params.getChiCsmUnitDescr() != null ? params.getChiCsmUnitDescr().replaceAll("\\s+$", "") : "";
                        // 模拟 replace 函数
                        W_OUTWORD = W_WORD.replace("CU.M", trimmedDescr);
                        zs810Response.setDescrOnBill(W_OUTWORD);
                    } else {
                        W_OUTWORD = W_WORD.replace("CU.M ", "cu.m ");
                        zs810Response.setDescrOnBill(W_OUTWORD);
                    }
                    break;
                }
            }
            if ("N".equals(v_sd_found_in_list1_sw)) {
                for (Zs670SetupSqlSList2BatchDto item2 : piList2) {
                    if ("Y".equals(v_sd_found_in_list2_sw)) {
                        break;
                    }
                    if (svcId.equals(item2.getSvcId())) {
                        gvBillExtractionHeaderBatch1.setSortKeyPreId(item2.getPreId());
                        gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item2.getPrePrintPriority());
                        gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item2.getBillPrtPrioFlg());
                        v_sd_found_in_list2_sw = "Y";
                        item2.setSubTotalSw("Y");
                        break;
                    }
                }
            }
            // 模拟 nvl 函数
            String nvlBitemId = zs810Response.getBitemId() != null ? zs810Response.getBitemId() : " ";
            String nvlTempBitemId = W_TEMP_BITEM_ID != null ? W_TEMP_BITEM_ID : " ";

            if (nvlBitemId.equals(nvlTempBitemId)) {
                // 模拟 nvl 函数
                String nvlEndDt = zs810Response.getEndDt() != null ? zs810Response.getEndDt() : " ";
                String nvlTempEndDt = W_TEMP_BITEM_END_DT != null ? W_TEMP_BITEM_END_DT : " ";

                if (nvlEndDt.equals(nvlTempEndDt)) {
                    W_TEMP_BITEM_CALCLN_SEQ1++;
                } else {
                    W_TEMP_BITEM_CALCHDR_SEQ++;
                    if (W_TEMP_BITEM_CALCHDR_SEQ > 99) {
                        W_TEMP_BITEM_CALCHDR_SEQ = 1;
                    }
                    W_TEMP_BITEM_CALCLN_SEQ1 = 1;
                }
            } else {
                W_TEMP_BITEM_CALCHDR_SEQ = zs810Response.getHeaderSeqNo();
                W_TEMP_BITEM_CALCLN_SEQ1 = 1;
            }
            gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(W_TEMP_BITEM_CALCHDR_SEQ);
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(String.format("%04d", W_TEMP_BITEM_CALCLN_SEQ1));
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3210");
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("70");
            gvBillExtractionHeaderBatch1.setSortKeySdGrp("20");
            gvBillExtractionHeaderBatch1.setSortKeyBitemGrp("20");
            gvBillExtractionHeaderBatch1.setSortKeySvcId(zs810Response.getSvcId());
            gvBillExtractionHeaderBatch1.setSortKeyBitemId(zs810Response.getBitemId());
            // 模拟 nvl 函数
            gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(zs810Response.getEndDt() != null ? zs810Response.getEndDt() : " ");
            if ("BPI-BSEG".equals(zs810Response.getAttrTypeCd()) && "Y".equals(zs810Response.getAttrVal())) {
                v_calc_amt = 0;
            } else {
                v_calc_amt = zs810Response.getCalcAmt();
            }
            //调用ctDetailForPrinterKey321方法组装数据
            String v_bp_extr_dtl = ctDetailForPrinterKey3210(zs810Response.getBitemId(), W_TEMP_BITEM_CALCHDR_SEQ, String.format("%03d", W_TEMP_BITEM_CALCLN_SEQ1),
                    zs810Response.getDescrOnBill(), v_calc_amt);
            //调用construct_bill_extr_line函数
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
            W_TEMP_BITEM_ID = zs810Response.getBitemId();
            W_TEMP_BITEM_END_DT = zs810Response.getEndDt();
        }
        gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemId(" ");
        gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(0);
        gvBillExtractionHeaderBatch1.setSortKeyBitemGrp(" ");
        resultMap.put("po_list1", piList1);
        resultMap.put("po_list2", piList2);
        return resultMap;
    }

    public List<Zs810SetupSqlSBsegclBatchResponse> zs810SetupSqlSBsegcl(String piBillId, String piAttrTypeCd, Integer ignorePrintSw) {
        List<Zs810SetupSqlSBsegclBatchResponse> retVal = new ArrayList<>();
        Set<String> added = new HashSet<>();
        List<Zs810SetupSqlSBsegclBatchResponse> existings = billExtrMapper.zs810SetupSqlSBsegcl(piBillId, piAttrTypeCd, ignorePrintSw);
        for (Zs810SetupSqlSBsegclBatchResponse item : existings) {
            if (!added.contains(item.getDescrOnBill())) {
                retVal.add(item);
            }
            added.add(item.getSvcId() + "-" + item.getDescrOnBill());
        }
        return retVal;
    }

    public Map<String, Object> me040CreateRecord3300(
            String piUserId, Date piProcessDttm, BillEntity piBillRow, String piLanguageCd,
            List<Mq010GetList1BtachResponse> piList1, List<Zs670SetupSqlSList2BatchDto> piList2,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList, List<FinTranWithAdj> finTranWithAdjList) {
        Map<String, Object> resultMap = new HashMap<>();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        AlgorithmParameters params = algorithmParams.get();
        String v_descr_on_bill = null;
        //调用SQ710_GET_ADJ.ZS820_SETUP_SQL_S_ADJ存储过程
        List<Zs820SetupSqlSAdjBatchResponse> zs820Responses = zs820SetupSqlSAdj(
                piBillRow.getBillId(), params.getDepositAdjCd(), params.getOverpaySdType(), piLanguageCd,
                params.getInstFldName(), params.getDisputeFldName(),
                svcDtlWithTypeAndBalList, finTranWithAdjList);
        for (Zs820SetupSqlSAdjBatchResponse zs820Response : zs820Responses) {
            String v_sd_found_in_list1_sw = "N";
            String v_sd_found_in_list2_sw = "N";
            for (Mq010GetList1BtachResponse item : piList1) {
                if ("Y".equals(v_sd_found_in_list1_sw)) {
                    break;
                }
                if (zs820Response.getSvcId().equals(item.getSvcId())) {
                    gvBillExtractionHeaderBatch1.setSortKeyPreId(item.getPreId());
                    gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item.getPrePrintPriority());
                    gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item.getBillPrtPrioFlg());
                    v_sd_found_in_list1_sw = "Y";
                    item.setSubTotalSw("Y");
                    break;
                }
            }
            if ("N".equals(v_sd_found_in_list1_sw)) {
                for (Zs670SetupSqlSList2BatchDto item2 : piList2) {
                    if ("Y".equals(v_sd_found_in_list2_sw)) {
                        break;
                    }
                    if (zs820Response.getSvcId().equals(item2.getSvcId())) {
                        gvBillExtractionHeaderBatch1.setSortKeyPreId(item2.getPreId());
                        gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item2.getPrePrintPriority());
                        gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item2.getBillPrtPrioFlg());
                        v_sd_found_in_list2_sw = "Y";
                        item2.setSubTotalSw("Y");
                        break;
                    }
                }

            }
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3300");
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("70");
            gvBillExtractionHeaderBatch1.setSortKeySdGrp("30");
            gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(" ");
            gvBillExtractionHeaderBatch1.setSortKeyBitemId(" ");
            gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(0);
            gvBillExtractionHeaderBatch1.setSortKeyBitemGrp(" ");
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(" ");
            gvBillExtractionHeaderBatch1.setSortKeySvcId(zs820Response.getSvcId());
            //调用SQ711_CHECK_ADJ.ZS821_SETUP_SQL_S_CHKADJ存储过程
            Integer v_rec_cnt = zs821SetupSqlSChkadj(zs820Response.getSvcId(), piBillRow.getBillId());
            if (v_rec_cnt > 0) {
                //調用SQ712_GET_ADJ_AMT.ZS822_SETUP_SQL_S_ADJAMT存储过程
                v_descr_on_bill = zs822SetupSqlSAdjc("SBDC", piLanguageCd);
            }
            //调用ctDetailForPrinterKey3300方法组装数据
            String v_bp_extr_dtl = ctDetailForPrinterKey3300(zs820Response.getDescrOnBill(), zs820Response.getCurAmt());
            //调用construct_bill_extr_line函数获取v_bp_extr_lines值
            String premiseId = null;
            for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
                if (svc.getSvcId().equals(zs820Response.getSvcId())) {
                    premiseId = svc.getGeoAddressId();
                    break;
                }
            }
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl, premiseId);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        }
        resultMap.put("po_list1", piList1);
        resultMap.put("po_list2", piList2);
        return resultMap;
    }

    public List<Zs820SetupSqlSAdjBatchResponse> zs820SetupSqlSAdj(
            String piBillId, String piAdjTypeCd, String piSvcTypeCd,
            String piLanguageCd, String piFieldName1, String piFieldName2,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<FinTranWithAdj> finTranWithAdjList) {
        List<Zs820SetupSqlSAdjBatchResponse> retVal = new ArrayList<>();
        Map<String, Object[]> groups = new HashMap<>();
        for (FinTranWithAdj finTran : finTranWithAdjList) {
            if (finTran.getAdjId() == null) {
                continue;
            }
            if (!"AD".equals(finTran.getFinTranTypeInd()) && !"AX".equals(finTran.getFinTranTypeInd())) {
                continue;
            }
            if (!"Y".equals(finTran.getShowOnBillSw())) {
                continue;
            }
            if (piAdjTypeCd.equals(finTran.getAdjTypeCode())) {
                // TODO
                continue;
            }
//            boolean isContinuing = false;
//            for (SvcDtlWithTypeAndBal svcDtl : svcDtlWithTypeAndBalList) {
//                if (svcDtl.getSvcId().equals(finTran.getSvcId())) {
//                    if (svcDtl.getSvcTypeCode().equals(piSvcTypeCd)) {
//                        isContinuing = true;
//                        break;
//                    }
//                }
//                // TODO
//            }
//            if (isContinuing) {
//                continue;
//            }
            Object[] vals = null;
            String key = finTran.getSvcId() + "#" + finTran.getRelatedId();
            if (groups.containsKey(key)) {
                vals = groups.get(key);
            } else {
                vals = new Object[]{BigDecimal.ZERO, ""};
            }
            if ("ENG".equals(piLanguageCd)) {
                vals[1] = finTran.getAdjTypeDescOnBill();
            } else {
                vals[1] = finTran.getAdjTypeDescOnBillTc();
            }
            vals[0] = ((BigDecimal)vals[0]).add(finTran.getCurAmt());
            groups.put(key, vals);
        }
        for (Map.Entry<String,Object[]> entry : groups.entrySet()) {
            Zs820SetupSqlSAdjBatchResponse res = new Zs820SetupSqlSAdjBatchResponse();
            res.setSvcId(entry.getKey().substring(0, entry.getKey().indexOf("#")));
            res.setCurAmt(((BigDecimal)entry.getValue()[0]).doubleValue());
            res.setDescrOnBill((String)entry.getValue()[1]);
            retVal.add(res);
        }
        return retVal;
//        return billExtrMapper.zs820SetupSqlSAdj(piBillId, piAdjTypeCd, piSvcTypeCd, piLanguageCd, piFieldName1, piFieldName2);
    }

    @Override
    public Integer zs821SetupSqlSChkadj(String piSvcId, String piBillId) {
        return billExtrMapper.zs821SetupSqlSChkadj(piSvcId, piBillId);
    }

    @Override
    public String zs822SetupSqlSAdjc(String piSvcId, String piBillId) {
        return billExtrMapper.zs822SetupSqlSAdjc(piSvcId, piBillId);
    }

    public Map<String, Object> me050CreateRecord3100(
            String piUserId, Date piProcessDttm, BillEntity piBillRow, String piLanguageCd,
            List<Mq010GetList1BtachResponse> piList1, List<Zs670SetupSqlSList2BatchDto> piList2,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList, List<FinTranWithAdj> finTranWithAdjList) {
        Map<String, Object> resultMap = new HashMap<>();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        AlgorithmParameters params = algorithmParams.get();
        String v_sub_total_sw = null;
        String v_sd_descr = null;
        //调用SQ720_GET_SA.ZS830_SETUP_SQL_S_SA存储过程
        List<Zs830SetupSqlSSaBatchResponse> zs830Responses = zs830SetupSqlSSa(
                piBillRow.getBillId(), params.getDepositAdjCd(), piBillRow.getAccountId(),
                params.getInstFldName(), params.getDisputeFldName(), params.getOverpaySdType(),
                svcDtlWithTypeAndBalList, finTranWithAdjList);
        Map<String, String> billPrintPrios = new HashMap<>();
        svcDtlWithTypeAndBalList.sort((o1, o2) -> {
            if (o1.getBillPrintPrio() == null) {
                return 1;
            }
            if (o2.getBillPrintPrio() == null) {
                return -1;
            }
            int cmp = o1.getBillPrintPrio().compareTo(o2.getBillPrintPrio());
            if (cmp != 0) {
                return cmp;
            }
            return o1.getCreatedDate().compareTo(o2.getCreatedDate());
        });
        int index = 1;
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            billPrintPrios.put(svc.getSvcId(), String.format("%02d", index++));
        }
        for (Zs830SetupSqlSSaBatchResponse zs830Response : zs830Responses) {
            v_sd_descr = null;
            String v_sd_found_in_list1_sw = "N";
            String v_sd_found_in_list2_sw = "N";
            for (Mq010GetList1BtachResponse item : piList1) {
                if ("Y".equals(v_sd_found_in_list1_sw)) {
                    break;
                }
                if (zs830Response.getSaId().equals(item.getSvcId())) {
                    gvBillExtractionHeaderBatch1.setSortKeyPreId(item.getPreId());
                    gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item.getPrePrintPriority());
                    gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item.getBillPrtPrioFlg());
                    v_sub_total_sw = item.getSubTotalSw();
                    if ("BC".equals(item.getSpecialRoleFlg())) {
                        //调用SQ740_GET_SA_DESC.ZS850_SETUP_SQL_S_SADESC存储过程
                        String v_descr_on_bill = zs850SetupSqlSSadesc(zs830Response.getSaId(), piBillRow.getBillId());
                        if (StringUtil.isNotEmpty(v_descr_on_bill)) {
                            v_sd_descr = v_descr_on_bill;
                        } else {
                            v_sd_descr = item.getDfltDescrOnBill();
                        }
                    } else {
                        v_sd_descr = item.getDfltDescrOnBill();
                        // FIXME
                        if (v_sd_descr.trim().isEmpty()) {
                            if (piLanguageCd.equals("ZHT")) {
                                v_sd_descr = "款額轉撥";
                            } else {
                                v_sd_descr = "Transfer Adjustment";
                            }
                            v_sub_total_sw = "Y";
                        }
                    }
                    v_sd_found_in_list1_sw = "Y";
                }
            }
            if ("N".equals(v_sd_found_in_list1_sw)) {
                for (Zs670SetupSqlSList2BatchDto item2 : piList2) {
                    if ("Y".equals(v_sd_found_in_list2_sw)) {
                        break;
                    }
                    if (zs830Response.getSaId().equals(item2.getSvcId())) {
                        gvBillExtractionHeaderBatch1.setSortKeyPreId(item2.getPreId());
                        gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(item2.getPrePrintPriority());
                        gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(item2.getBillPrtPrioFlg());
                        v_sub_total_sw = item2.getSubTotalSw();
                        if ("BC".equals(item2.getSpecialRoleFlg())) {
                            //调用SQ740_GET_SA_DESC.ZS850_SETUP_SQL_S_SADESC存储过程
                            String v_descr_on_bill = zs850SetupSqlSSadesc(zs830Response.getSaId(), piBillRow.getBillId());
                            if (StringUtil.isNotEmpty(v_descr_on_bill)) {
                                v_sd_descr = v_descr_on_bill;
                            } else {
                                v_sd_descr = item2.getDfltDescrOnBill();
                            }
                        } else {
                            v_sd_descr = item2.getDfltDescrOnBill();
                        }
                        v_sd_found_in_list2_sw = "Y";
                    }
                }
            }
            // FIXME
            if (v_sd_descr == null) {
                if (piLanguageCd.equals("ZHT")) {
                    v_sd_descr = "款額轉撥";
                } else {
                    v_sd_descr = "Transfer Adjustment";
                }
                v_sub_total_sw = "Y";
            }
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3100");
            gvBillExtractionHeaderBatch1.setSortKeyBillRecGrp("50");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("70");
            gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority(billPrintPrios.get(zs830Response.getSaId()));
            gvBillExtractionHeaderBatch1.setSortKeySvcId(zs830Response.getSaId());
            gvBillExtractionHeaderBatch1.setSortKeySdGrp("10");
            gvBillExtractionHeaderBatch1.setSortKeyBitemEndDate(" ");
            gvBillExtractionHeaderBatch1.setSortKeyBitemId(" ");
            gvBillExtractionHeaderBatch1.setSortKeyBitemCalchdrSeq(0);
            gvBillExtractionHeaderBatch1.setSortKeyBitemGrp(" ");
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(" ");
            gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(billPrintPrios.get(zs830Response.getSaId()));
            //调用SQ750_GET_DSDCHG.ZS860_SETUP_SQL_S_DSDCHG存储过程
            String v_dsd_charge_sw = zs860SetupSqlSDsdchg(zs830Response.getSaId(), params.getDsdChargeCd(), "SEWAGE", "TES");
            //调用ctDetailForPrinterKey3100方法组装数据
            String v_bp_extr_dtl = ctDetailForPrinterKey3100(zs830Response.getSaId(), v_sd_descr, zs830Response.getSaSubtotal(), v_sub_total_sw,
                    v_dsd_charge_sw);
            //调用construct_bill_extr_line函数
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3100");
            String premiseId = null;
            for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
                if (svc.getSvcId().equals(zs830Response.getSaId())) {
                    premiseId = svc.getGeoAddressId();
                    break;
                }
            }
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl, premiseId);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("3400");
            gvBillExtractionHeaderBatch1.setSortKeySdGrp("40");
            //调用ctDetailForPrinterKey3400方法组装数据
            v_bp_extr_dtl = "";
            //调用construct_bill_extr_line函数
            v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
        }
        resultMap.put("po_list1", piList1);
        resultMap.put("po_list2", piList2);
        return resultMap;
    }

//    @Override
    public List<Zs830SetupSqlSSaBatchResponse> zs830SetupSqlSSa(
            String piBillId, String piAdjTypeCd, String piAcctId,
            String piFieldName1, String piFieldName2, String piSdTypeCd,
            List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList,
            List<FinTranWithAdj> finTranWithAdjList) {
        Set<String> finTranTypeIds = new HashSet<>();
        for (FinTranWithAdj ft : finTranWithAdjList) {
            if (ft.getFinTranTypeId() == null) {
                continue;
            }
            finTranTypeIds.add(ft.getFinTranTypeId());
        }
        return billExtrMapper.zs830SetupSqlSSa(piBillId, finTranTypeIds,
            piAdjTypeCd, piAcctId, piFieldName1, piFieldName2, piSdTypeCd);
    }

    @Override
    public String zs850SetupSqlSSadesc(String piSvcId, String piBillId) {
        return billExtrMapper.zs850SetupSqlSSadesc(piSvcId, piBillId);
    }

    @Override
    public String zs860SetupSqlSDsdchg(String piSvcId, String piAttrTypeCd, String piAttrValSewage, String piAttrValTes) {
        return billExtrMapper.zs860SetupSqlSDsdchg(piSvcId, piAttrTypeCd, piAttrValSewage, piAttrValTes);
    }

    public void me060CreateRecord2200(String piUserId, Date piProcessDttm, BillEntity piBillRow,
                                      List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
//        AlgorithmParameters params = algorithmParams.get();
        //调用SQ340_GET_CSMGPH.ZS430_SETUP_SQL_S_CSMGPH存储过程
        List<Zs430SetupSqlSCsmgphBatchResponse> zs430Responses = zs430SetupSqlSCsmgph(piBillRow.getAccountId(), piBillRow.getBillId(),
                piBillRow.getCmpltDt(), piBillRow.getBillDt(), "CM", svcDtlWithTypeAndBalList);
        String v_temp_pre_id = "";
        List<Zs870TypeflgDataDto> vTypeflg = new ArrayList<>();
        List<Zs870VcsmgphDataDto> vCsmgph = new ArrayList<>();
        int v_csmgph_cnt = 0;
        Set<String> bitemIds = new HashSet<>();
        for (Zs430SetupSqlSCsmgphBatchResponse zs430Response : zs430Responses) {
            bitemIds.add(zs430Response.getBitemId());
        }
        if (bitemIds.isEmpty()) {
            return;
        }
        Map<String, BitemReadEntity> bitemReadCache = new HashMap<>();
        List<BitemReadEntity> bitemReads = bitemReadMapper.selectList(new QueryWrapper<BitemReadEntity>().lambda().in(
            BitemReadEntity::getBitemId, bitemIds));
        for (BitemReadEntity read : bitemReads) {
            BitemReadEntity oldRead = bitemReadCache.get(read.getBitemId());
            if (oldRead == null) {
                bitemReadCache.put(read.getBitemId(), read);
            } else if (oldRead.getSeq() > read.getSeq()) {
                bitemReadCache.put(read.getBitemId(), read);
            }
        }
        int index = 1;
        for (Zs430SetupSqlSCsmgphBatchResponse zs430Response : zs430Responses) {
            BitemReadEntity read = bitemReadCache.get(zs430Response.getBitemId());
            if (read == null) {
                continue;
            }
            gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2200");
            gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
            gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("30");
            gvBillExtractionHeaderBatch1.setSortKeyPreId(zs430Response.getPreId());
            gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
            gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
            gvBillExtractionHeaderBatch1.setSortKeySdGrp(" ");
            gvBillExtractionHeaderBatch1.setSortKeyLineSeq(String.format("%04d", index++));

            String vReadTypeInd = "";
            if ("50".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "S";
            } else if ("30".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "E";
            } else if ("35".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "E";
            } else if ("40".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "E";
            } else if ("80".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "F";
            } else if ("70".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "A";
            } else if ("60".equals(read.getEndReadTypeInd())) {
                vReadTypeInd = "A";
            } else {
                vReadTypeInd = "A";
            }
            //调用ctDetailForPrinterKey2200方法组装数据
            String v_bp_extr_dtl = ctDetailForPrinterKey2200(
                zs430Response.getMonthYy(), vReadTypeInd,
                zs430Response.getAvgCsm());
            //调用construct_bill_extr_line函数
            String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
            //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
            mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
            if (!v_temp_pre_id.equals(zs430Response.getPreId())) {
                v_temp_pre_id = zs430Response.getPreId();
                // 调用SQ760_GET_TYPEFLG.ZS870_SETUP_SQL_S_TYPEFLG存储过程
//                List<Zs870SetupSqlSTypeflgBatchResponse> zs870Responses = null;
//                if (!v_temp_pre_id_cache.containsKey(v_temp_pre_id)) {
//                    zs870Responses = zs870SetupSqlSTypeflg(piBillRow.getAccountId(), piBillRow.getBillId(),
//                            piBillRow.getCompDate(), piBillRow.getBillDate(), v_temp_pre_id);
//                    v_temp_pre_id_cache.put(v_temp_pre_id, zs870Responses);
//                } else {
//                    zs870Responses = v_temp_pre_id_cache.get(v_temp_pre_id);
//                }
//                int v_typeflg_cnt = 0;
//                for (Zs870SetupSqlSTypeflgBatchResponse zs870Response : zs870Responses) {
//                    if (v_typeflg_cnt < 7) {
//                        v_typeflg_cnt++;
//                        String vTypeflgMonthYY = zs870Response.getMonth();
//                        String vReadTypeInd = zs870Response.getReadTypeInd();
//                        vTypeflg.add(new Zs870TypeflgDataDto(vTypeflgMonthYY, vReadTypeInd));
//                    }
//                }
                vCsmgph.add(new Zs870VcsmgphDataDto(zs430Response.getPreId(), zs430Response.getMonthYy(), zs430Response.getCmsDays(), zs430Response.getBillSq(),
                        zs430Response.getAvgCsm()));
            } else {
                vCsmgph.add(new Zs870VcsmgphDataDto(zs430Response.getPreId(), zs430Response.getMonthYy(), zs430Response.getCmsDays(), zs430Response.getBillSq(),
                        zs430Response.getAvgCsm()));
            }
        }
//        if (StringUtil.isNotEmpty(v_temp_pre_id)) {
//            //调用SQ760_GET_TYPEFLG.ZS870_SETUP_SQL_S_TYPEFLG存储过程
//            List<Zs870SetupSqlSTypeflgBatchResponse> zs870Responses = zs870SetupSqlSTypeflg(piBillRow.getAccountId(), piBillRow.getBillId(),
//                    piBillRow.getCompDate(), piBillRow.getBillDate(), v_temp_pre_id);
//            int v_typeflg_cnt = 0;
//            for (Zs870SetupSqlSTypeflgBatchResponse zs870Response : zs870Responses) {
//                if (v_typeflg_cnt < 7) {
//                    v_typeflg_cnt++;
//                    String vTypeflgMonthYY = zs870Response.getMonth();
//                    String vReadTypeInd = zs870Response.getReadTypeInd();
//                    vTypeflg.add(new Zs870TypeflgDataDto(vTypeflgMonthYY, vReadTypeInd));
//                }
//            }
//            int v_idx = 1;
//            for (int vTempCnt = 1; vTempCnt <= vCsmgph.size(); vTempCnt++) {
//                gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2200");
//                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
//                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("30");
//                gvBillExtractionHeaderBatch1.setSortKeyPreId(v_temp_pre_id);
//                gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
//                gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
//                gvBillExtractionHeaderBatch1.setSortKeySdGrp(" ");
//                gvBillExtractionHeaderBatch1.setSortKeyLineSeq(String.format("%04d", vTempCnt));
//                String v_READ_TYPE_IND = "";
//
//                if (v_idx <= vTypeflg.size()) {
//                    if (vTypeflg.get(v_idx - 1).getMonthYY().equals(vCsmgph.get(vTempCnt - 1).getMonthYY())) {
//                        v_READ_TYPE_IND = vTypeflg.get(v_idx - 1).getReadTypeInd();
//                        v_idx++;
//                    }
//                }
//                //调用ctDetailForPrinterKey2200方法组装数据
//                String v_bp_extr_dtl = ctDetailForPrinterKey2200(vCsmgph.get(vTempCnt - 1).getMonthYY(), v_READ_TYPE_IND,
//                        vCsmgph.get(vTempCnt - 1).getAvgCSM());
//                //调用construct_bill_extr_line函数
//                String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
//                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
//                mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
//            }
//        }
    }

    public List<Zs430SetupSqlSCsmgphBatchResponse> zs430SetupSqlSCsmgph(String piAcctId, String piBillId,
                                                                        Date piCompleteDttm, Date piBillDt, String piCmSqi,
                                                                        List<SvcDtlWithTypeAndBal> svcDtlWithTypeAndBalList) {
        Set<String> svcTypeCodes = new HashSet<>();
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            if (svc.getGeoAddressId() == null) {
                continue;
            }
            svcTypeCodes.add(svc.getSvcTypeCode());
        }
        if (svcTypeCodes.isEmpty()) {
            return Collections.emptyList();
        }
        List<CfgSvcTpAttrEntity> svcTypeAttrs = cfgSvcTpAttrMapper.selectList(
            new QueryWrapper<CfgSvcTpAttrEntity>().lambda().in(CfgSvcTpAttrEntity::getSvcTpCd, svcTypeCodes));
        String onlyOneSvcTypeCode = "";
        for (CfgSvcTpAttrEntity attr : svcTypeAttrs) {
            if ("CONSATY".equals(attr.getPropTpCd()) && "Y".equals(attr.getPropVal())) {
                onlyOneSvcTypeCode = attr.getSvcTpCd();
                break;
            }
        }
        SvcDtlWithTypeAndBal onlyOneSvcDtl = null;
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            if (onlyOneSvcTypeCode.equals(svc.getSvcTypeCode())) {
                onlyOneSvcDtl = svc;
            }
        }
        Set<String> svcIds = new HashSet<>();
        Set<String> premiseIds = new HashSet<>();
        for (SvcDtlWithTypeAndBal svc : svcDtlWithTypeAndBalList) {
            svcIds.add(svc.getSvcId());
            if (svc.getGeoAddressId() != null) {
                premiseIds.add(svc.getGeoAddressId());
            }
        }
        if (premiseIds.isEmpty()) {
            return Collections.emptyList();
        }
        Integer count = billExtrMapper.zs430SetupSqlSCsmgphFront(piBillId, svcIds, premiseIds);
        if (count == 0) {
            return Collections.emptyList();
        }

        return billExtrMapper.zs430SetupSqlSCsmgph_improved(onlyOneSvcDtl.getSvcId(), onlyOneSvcDtl.getGeoAddressId(),
            piBillId, piCompleteDttm, piBillDt, piCmSqi);
    }

    @Override
    public List<Zs870SetupSqlSTypeflgBatchResponse> zs870SetupSqlSTypeflg(String piAcctId, String piBillId, Date piCompleteDttm, Date piBillDt, String piPremiseId) {
        return billExtrMapper.zs870SetupSqlSTypeflg(piAcctId, piBillId, piCompleteDttm, piBillDt, piPremiseId);
    }

    public void me070CreateRecord2300(String piUserId, Date piProcessDttm, BillEntity piBillRow,
                                      List<BitemWithReadAndQty> bitemWithReadAndQtyList,
                                      List<FinTranWithAdj> finTranWithAdjList) {
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        //调用SQ350_GET_CSMREC_v2.ZS440_SETUP_SQL_S_CSMREC_v2存储过程
        Zs440SetupSqlSCsmrecV2BatchResponse zs440ResponsesV2 = zs440SetupSqlSCsmrecV2(
                piBillRow.getBillId(), bitemWithReadAndQtyList, finTranWithAdjList);
        //调用SQ350_GET_CSMREC.ZS440_SETUP_SQL_S_CSMREC存储过程
        List<Zs440SetupSqlSCsmrecV2BatchResponse> zs440Responses = zs440SetupSqlSCsmrec(
                piBillRow.getBillId(), bitemWithReadAndQtyList, finTranWithAdjList);
        double w_csm = 0;
        double v_gallons = 0;
        double v_adc_liter = 0;
        for (Zs440SetupSqlSCsmrecV2BatchResponse zs440Response : zs440Responses) {
            if ("Y".equals(gvGlobalVariableBatch1.getGv_debug_mode())) {
                if (zs440Response.getV2Csm() != zs440ResponsesV2.getV2Csm() && zs440ResponsesV2.getV2Csm() != 0) {
                    //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
                    zz000SetupSqlICiMsg(piBillRow.getBillId(), "20",
                            "ME070_CREATE_RECORD_2300 Wrong 2300, should fix multi BillSeg consumption v2_csm " + zs440ResponsesV2.getV2Csm());
                }
                zs440ResponsesV2.setV2Csm(0);
            }
            if ("GAL".equals(zs440Response.getUomCd())) {
                w_csm = zs440Response.getV2Csm();
            } else {
                gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("2300");
                gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("40");
                gvBillExtractionHeaderBatch1.setSortKeyPrePrintPriority("00");
                gvBillExtractionHeaderBatch1.setSortKeyPreId(zs440Response.getPreId());
                gvBillExtractionHeaderBatch1.setSortKeySvcId(" ");
                gvBillExtractionHeaderBatch1.setSortKeyLineSeq(" ");
                gvBillExtractionHeaderBatch1.setSortKeySdPrintPriority(" ");
                gvBillExtractionHeaderBatch1.setSortKeySdGrp(" ");
                if (w_csm * 4.54609 == zs440Response.getV2Csm()) {
                    v_gallons = w_csm * 1000;
                } else {
                    v_gallons = 0;
                }
                if ("Y".equals(gvGlobalVariableBatch1.getW_adc_sw())) {
                    v_adc_liter = zs440Response.getAvgCsm() * 1000;
                } else {
                    v_adc_liter = 0;
                }
                //调用ctDetailForPrinterKey2300方法组装数据
                String v_bp_extr_dtl = ctDetailForPrinterKey2300(zs440Response.getCsmDays(), trunc(v_gallons, 6),
                        trunc(zs440Response.getV2Csm(), 6), trunc(zs440Response.getAvgCsm(), 6),
                        gvGlobalVariableBatch1.getW_adc_sw(), trunc(v_adc_liter, 6));
                //调用construct_bill_extr_line函数
                String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
                //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
                mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
                w_csm = 0;
            }
        }
    }

    /**
     * 水费消耗量
     */
    public Zs440SetupSqlSCsmrecV2BatchResponse zs440SetupSqlSCsmrecV2(
            String piBillId,
            List<BitemWithReadAndQty> bitemWithReadAndQtyList,
            List<FinTranWithAdj> finTranWithAdjList) {
        List<Zs440SetupSqlSCsmrecV2BatchResponse> resps = zs440SetupSqlSCsmrec(
                piBillId, bitemWithReadAndQtyList, finTranWithAdjList);
        if (resps.isEmpty()) {
            return null;
        }
        return resps.get(0);
//        return billExtrMapper.zs440SetupSqlSCsmrecV2(piBillId);
    }

    /**
     * 水费消耗量
     */
    public List<Zs440SetupSqlSCsmrecV2BatchResponse> zs440SetupSqlSCsmrec(
            String piBillId,
            List<BitemWithReadAndQty> bitemWithReadAndQtyList,
            List<FinTranWithAdj> finTranWithAdjList) {
        List<Zs440SetupSqlSCsmrecV2BatchResponse> retVal = new ArrayList<>();
        Map<String, List<BitemWithReadAndQty>> groupByPremiseIds = new HashMap<>();
        for (BitemWithReadAndQty row : bitemWithReadAndQtyList) {
            if (!"50".equals(row.getBitemStsInd()) && !"60".equals(row.getBitemStsInd())) {
                continue;
            }
            if (!"+".equals(row.getCalcUsageInd())) {
                continue;
            }
            if (!in(row.getUomCode(), "CM", "GAL")) {
                continue;
            }
            if ("60".equals(row.getBitemStsInd()) && existBxFinTran(row.getBitemId(), finTranWithAdjList)) {
                continue;
            }
            String geoAddressId = row.getGeoAddressId();
            List<BitemWithReadAndQty> bitems;
            if (groupByPremiseIds.containsKey(geoAddressId)) {
                bitems = groupByPremiseIds.get(geoAddressId);
                boolean found = false;
                for (BitemWithReadAndQty item : bitems) {
                    if (item.getEndMeterReadId().equals(row.getEndMeterReadId())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    bitems.add(row);
                }
            } else {
                bitems = new ArrayList<>();
                bitems.add(row);
                groupByPremiseIds.put(geoAddressId, bitems);
            }
        }
        for (Map.Entry<String, List<BitemWithReadAndQty>> entry : groupByPremiseIds.entrySet()) {
            List<BitemWithReadAndQty> bitems = entry.getValue();
            Zs440SetupSqlSCsmrecV2BatchResponse res = new Zs440SetupSqlSCsmrecV2BatchResponse();
            res.setPreId(entry.getKey());
            res.setUomCd(bitems.get(0).getUomCode());
            res.setV2Csm(0.0);
            Date min = null;
            Date max = null;
            for (BitemWithReadAndQty bitem : bitems) {
                if (min == null) {
                    min = bitem.getCalcStartDate();
                }
                if (max == null) {
                    max = bitem.getCalcEndDate();
                }
                if (bitem.getBillableBillQty() != null && res.getV2Csm() == 0.0) {
                    res.setV2Csm(res.getV2Csm() + bitem.getBillableBillQty().doubleValue());
                }
                if (bitem.getCalcStartDate().before(min)) {
                    min = bitem.getCalcStartDate();
                }
                if (bitem.getCalcEndDate().after(max)) {
                    max = bitem.getCalcEndDate();
                }
            }
            long days = Duration.between(min.toInstant(), max.toInstant()).toDays() + 1;
            res.setCsmDays((int)days);
            res.setAvgCsm(res.getV2Csm() / days);
            retVal.add(res);
        }
        // 这个实际上是按照premise分组的，账单上是显示汇总的

        return retVal;
//        return billExtrMapper.zs440SetupSqlSCsmrec(piBillId);
    }

    @Override
    public void mz010EndOfBill(String piUserId, Date piProcessDttm, BillEntity piBillRow) {
        GvBillExtractionHeaderBatch gvBillExtractionHeaderBatch1 = gvBillExtractionHeaderBatch.get();
        gvBillExtractionHeaderBatch1.setBillPrintPrinterKey("9999");
        gvBillExtractionHeaderBatch1.setSortKeyPreRecGrp("80");
        //调用ct_detail_for_printer_key_9999方法组装数据得到空字符串
        String v_bp_extr_dtl = "";
        //调用construct_bill_extr_line函数
        String v_bp_extr_lines = constructBillExtrLine(v_bp_extr_dtl);
        //调用MX010_WRITE_EXTRACT_DATA.NX010_WRITE_TO_TEMP.SZ010_WRITE_TO_TEMP.ZS620_SETUP_SQL_I_TEMPLN存储过程
        mx010WriteExtractData(piBillRow.getBillId(), v_bp_extr_lines, piUserId, piProcessDttm);
    }

    /*!
    ** 计算账单总金额。
    */
    @Override
    public BigDecimal fncCalcPayableAmount(String piBillId, String piAdjTypeCode, String piFieldName1, String piFieldName2, String piSvcTypeCode) {
        //调用FNC_GET_OPEN_ITEM_SW函数获取表CFG_CUST_CLS中的OPEN_ITEM_SW信息
        String v_OPEN_ITEM_SW = fncGetOpenItemSw(piBillId);
        //调用FNC_RETRIEVE_BILL_ROW函数获取BILL主表信息
        BillEntity v_bill_row = billMapper.selectById(piBillId);
        AlgorithmParameters parameters = new AlgorithmParameters();
        parameters.setDepositAdjCd(piAdjTypeCode);
        parameters.setInstFldName(piFieldName1);
        parameters.setDisputeFldName(piFieldName2);
        parameters.setOverpaySdType(piSvcTypeCode);
        BigDecimal v_TOT_AMT;
        //调用SQ150_GET_OBLBAL.ZS200_SETUP_SQL_S_OBLBAL存储过程
        if ("Y".equals(v_OPEN_ITEM_SW))
            v_TOT_AMT = zs200SetupSqlSOblbal(piBillId, v_bill_row.getAccountId(), parameters);
        else {
            //调用SQ010_GET_BLBAL.ZS170_SETUP_SQL_S_BLBAL存储过程
            v_TOT_AMT = zs170SetupSqlSBlbal(piBillId, v_bill_row.getAccountId(), parameters);
        }
        return v_TOT_AMT;
    }

    @Override
    public List<Zs180SetupSqlSBmsgprmBatchResponse> zs180SetupSqlSBmsgprm(String piBillId, String piBillMsgCd) {
        return billExtrMapper.zs180SetupSqlSBmsgprm(piBillId, piBillMsgCd);
    }

    public Map<String, Object> pa270SurchargeMaint(Date piProcessDttm, String piExtractType, String piBillId, String piSaId, double piTotalAmtDue,
                                    String piBillTier1Scd, String piBillTier2Scd,
                                    List<FinTranWithAdj> finTranWithAdjList,
                                    List<BitemWithReadAndQty> bitemWithReadAndQtyList) {
        //调用bill_extr_api_get_possible_surcharge_pkg.AA000_MAIN存储过程
        Map<String, Object> res = billExtrApiGetPossibleSurchargePkgService.aa000Main(piProcessDttm, piExtractType, piBillId,
                null, piBillTier1Scd, piTotalAmtDue, finTranWithAdjList, bitemWithReadAndQtyList);
        String v_error_code = "";
        String v_error_parm = "";
        if(StringUtil.isNotEmpty(v_error_code) || StringUtil.isNotEmpty(v_error_parm)){
            //调用ZZ000_SETUP_SQL_I_CI_MSG存储过程
            zz000SetupSqlICiMsg(piBillId, "30",
                    "PA270_SURCHARGE_MAINT CMPBSOMX:"+v_error_code+","+v_error_parm);
        }
        return res;
    }


    private static String rpad(String input, int length, char padChar) {
        if (input == null) {
            input = "";
        }
        StringBuilder sb = new StringBuilder(input);
        while (sb.length() < length) {
            sb.append(padChar);
        }
        return sb.toString();
    }

    /**
     * 格式化金额，添加正负号并左填充到 15 位
     *
     * @param amount 金额
     * @return 格式化后的金额字符串
     */
    private static String formatAmount(BigDecimal amount) {
        StringBuilder result = new StringBuilder();
        if (amount == null) {
            amount = BigDecimal.ZERO;
        }
        // 判断金额正负，添加相应符号
        if (amount.signum() < 0) {
            result.append("-");
        } else {
            result.append("+");
        }
        // 取绝对值并乘以 100
        BigDecimal absAmount = amount.abs().multiply(BigDecimal.valueOf(100));
        // 转换为整数并左填充到 15 位
        result.append(String.format("%015d", absAmount.longValue()));
        return result.toString();
    }

    private AccIdAndOpenItemSwDto getAccountIdAndOpenItemSw(String billId) {
        AccIdAndOpenItemSwDto account = billMapper.getAccountIdAndOpenItemSw(billId);
        return account;
    }

    private BigDecimal calculateTotalAmount(String billId, AccIdAndOpenItemSwDto account, AlgorithmParameters algParams) {
        BigDecimal tolAmt;
        if (YesOrNoEnum.YES.getCode().equals(account.getOpenItemSw())) {
            tolAmt = this.zs200SetupSqlSOblbal(billId, account.getAccountId(), algParams);
        } else {
            tolAmt = this.zs170SetupSqlSBlbal(billId, account.getAccountId(), algParams);
        }
        return tolAmt;
    }

    // 检查账单是否为最终账单
    private boolean checkFinalBill(String billId) {
        // 调用billPaymentMapper的checkFinalBill方法，传入账单id和最终账单消息码，返回结果
        int count = billExtrMapper.checkFinalBill(billId, "FNB1");
        // 如果结果大于0，则返回true，否则返回false
        return count > 0;
    }

    private String checkNonConforming(String billId, AlgorithmParameters algParams) {
        return billExtrMapper.checkNonConformingService(
                billId,
                algParams.getInstFldName(),
                algParams.getDisputeFldName()
        );
    }

    private BigDecimal determineFinalAmount(BigDecimal totalAmount, String openItemSw,
                                            boolean isFinalBill, String nonConSw, AlgorithmParameters algParams) {
        boolean conditionMet = totalAmount.compareTo(BigDecimal.valueOf(algParams.getMaxAmtDue())) >= 0
                || YesOrNoEnum.YES.getCode().equals(openItemSw)
                || isFinalBill
                || YesOrNoEnum.YES.getCode().equals(nonConSw);

        if (conditionMet) {
            return totalAmount.max(BigDecimal.ZERO);
        } else {
            return BigDecimal.ZERO;
        }
    }

    public String ctDetailForPrinterKey2000(String piPremiseId, String piPremSw, String piAddress1,
                                            String piAddress2, String piAddress3, String piAddress4,
                                            String piSerialNbr) {
        String vOutput;
        // 处理 piPremiseId
        String processedPremiseId = String.format("%10s", piPremiseId != null ? piPremiseId : "1").replace(' ', '0');
        // 处理 piPremSw
        String processedPremSw = String.format("%-1s", piPremSw != null ? piPremSw : " ");

        // 处理 piAddress1
        String processedAddress1 = padString(piAddress1, 64);
        // 处理 piAddress2
        String processedAddress2 = padString(piAddress2, 100);
        // 处理 piAddress3
        String processedAddress3 = padString(piAddress3, 100);
        // 处理 piAddress4
        String processedAddress4 = padString(piAddress4, 100);

        // 处理 piSerialNbr
        String processedSerialNbr = String.format("%-30s", piSerialNbr != null ? piSerialNbr : " ");

        vOutput = processedPremiseId + processedPremSw + processedAddress1 + processedAddress2 +
                processedAddress3 + processedAddress4 + processedSerialNbr;

        return vOutput.trim();
    }

    //将传入的字符串 str 填充空格
    private String padString(String str, int length) {
        if (str == null) {
            str = "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(str);
        while (sb.length() < length) {
            sb.append(' ');
        }
        return sb.substring(0, length);
    }

    //处理字符串，将其左填充到指定长度3500
    public static String ctDetailForPrinterKey3500(int piMsgNo, String piMsgOnBill) {
        String formattedMsgNo = String.format("%05d", piMsgNo == 0 ? 0 : piMsgNo);
        String lastTwoDigits = formattedMsgNo.substring(3, 5);

        String paddedMsgOnBill = piMsgOnBill != null ? piMsgOnBill : "";
        if (paddedMsgOnBill.length() < 254) {
            StringBuilder sb = new StringBuilder(paddedMsgOnBill);
            for (int i = paddedMsgOnBill.length(); i < 254; i++) {
                sb.append(' ');
            }
            paddedMsgOnBill = sb.toString();
        }
        paddedMsgOnBill = paddedMsgOnBill.substring(0, 254);

        StringBuilder sbForNull = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            sbForNull.append(' ');
        }
        String paddedNull = sbForNull.toString();

        String v_output = lastTwoDigits + paddedMsgOnBill + paddedNull;

        return v_output.trim();
    }

    //处理字符串，将其左填充到指定长度2100
    public static String ctDetailForPrinterKey2100(String piNatureOfSupply, String piSewageRate, String piSewageFactor, String piTesFactor, String piTesRate, String piHsicCd) {
        // 处理 piNatureOfSupply
        String processedNatureOfSupply = piNatureOfSupply != null ? piNatureOfSupply : "";
        if (processedNatureOfSupply.length() < 30) {
            StringBuilder natureSupplyBuilder = new StringBuilder(processedNatureOfSupply);
            for (int i = processedNatureOfSupply.length(); i < 30; i++) {
                natureSupplyBuilder.append(' ');
            }
            processedNatureOfSupply = natureSupplyBuilder.toString();
        }
        processedNatureOfSupply = processedNatureOfSupply.substring(0, 30);

        // 处理 piSewageRate
        String processedSewageRate = piSewageRate != null ? piSewageRate : "";
        processedSewageRate = processedSewageRate.length() > 5 ? processedSewageRate.substring(0, 5) : processedSewageRate;
        StringBuilder sewageRateBuilder = new StringBuilder(processedSewageRate);
        while (sewageRateBuilder.length() < 5) {
            sewageRateBuilder.append(' ');
        }
        processedSewageRate = sewageRateBuilder.toString();

        // 处理 piSewageFactor
        String processedSewageFactor = piSewageFactor != null ? piSewageFactor : "";
        processedSewageFactor = processedSewageFactor.length() > 5 ? processedSewageFactor.substring(0, 5) : processedSewageFactor;
        StringBuilder sewageFactorBuilder = new StringBuilder(processedSewageFactor);
        while (sewageFactorBuilder.length() < 5) {
            sewageFactorBuilder.append(' ');
        }
        processedSewageFactor = sewageFactorBuilder.toString();

        // 处理 piTesFactor
        String processedTesFactor = piTesFactor != null ? piTesFactor : "";
        processedTesFactor = processedTesFactor.length() > 5 ? processedTesFactor.substring(0, 5) : processedTesFactor;
        StringBuilder tesFactorBuilder = new StringBuilder(processedTesFactor);
        while (tesFactorBuilder.length() < 5) {
            tesFactorBuilder.append(' ');
        }
        processedTesFactor = tesFactorBuilder.toString();

        // 处理 piTesRate
        String processedTesRate = piTesRate != null ? piTesRate : "";
        processedTesRate = processedTesRate.length() > 5 ? processedTesRate.substring(0, 5) : processedTesRate;
        StringBuilder tesRateBuilder = new StringBuilder(processedTesRate);
        while (tesRateBuilder.length() < 5) {
            tesRateBuilder.append(' ');
        }
        processedTesRate = tesRateBuilder.toString();

        // 处理 piHsicCd
        String processedHsicCd = piHsicCd != null ? piHsicCd : "";
        processedHsicCd = processedHsicCd.length() > 8 ? processedHsicCd.substring(0, 8) : processedHsicCd;
        StringBuilder hsicCdBuilder = new StringBuilder(processedHsicCd);
        while (hsicCdBuilder.length() < 8) {
            hsicCdBuilder.append(' ');
        }
        processedHsicCd = hsicCdBuilder.toString();
        String vOutput = processedNatureOfSupply + processedSewageRate + processedSewageFactor + processedTesFactor + processedTesRate + processedHsicCd;
        return vOutput.trim();
    }

    public static String ctDetailForPrinterKey2400(String piMtrrecMETERNO, String piMtrrecSTARTREADDT,
                                                   double piMtrrecSTARTREADING, String piStartReadType,
                                                   String piMtrrecENDREADDT, double piMtrrecENDREADING,
                                                   String piEndReadType, String piMtrrecUSAGEFLG,
                                                   double piCsm, String piMtrrecUOCD, double piTotCsm,String accountId) {

        // 处理 pi_mtrrec_METER_NO
        String processedMeterNo = piMtrrecMETERNO != null ? piMtrrecMETERNO : "";
        processedMeterNo = String.format("%-30s", processedMeterNo);

        // 处理 pi_mtrrec_START_READ_DT
        String processedStartReadDt = piMtrrecSTARTREADDT != null ? piMtrrecSTARTREADDT : "";
        processedStartReadDt = String.format("%-10s", processedStartReadDt);

        // 处理 pi_mtrrec_START_READING
        String processedStartReading = "";
        if (Math.signum(piMtrrecSTARTREADING) == -1) {
            processedStartReading = "-";
        } else {
            processedStartReading = "+";
        }
        processedStartReading += String.format("%015d", (long) (Math.abs(piMtrrecSTARTREADING) * 1000000));

        // 处理 pi_start_read_type
        String processedStartReadType = piStartReadType != null ? piStartReadType : "";
        processedStartReadType = String.format("%-1s", processedStartReadType);

        // 处理 pi_mtrrec_END_READ_DT
        String processedEndReadDt = piMtrrecENDREADDT != null ? piMtrrecENDREADDT : "";
        processedEndReadDt = String.format("%-10s", processedEndReadDt);

        // 处理 pi_mtrrec_END_READING
        String processedEndReading = "";
        if (Math.signum(piMtrrecENDREADING) == -1) {
            processedEndReading = "-";
        } else {
            processedEndReading = "+";
        }
        processedEndReading += String.format("%015d", (long) (Math.abs(piMtrrecENDREADING) * 1000000));

        // 处理 pi_end_read_type
        String processedEndReadType = piEndReadType != null ? piEndReadType : "";
        processedEndReadType = String.format("%-1s", processedEndReadType);

        // 处理 pi_mtrrec_USAGE_FLG
        String processedUsageFlg = piMtrrecUSAGEFLG != null ? piMtrrecUSAGEFLG : "";
        processedUsageFlg = processedUsageFlg.length() > 0 ? processedUsageFlg.substring(0, 1) : " ";
        processedUsageFlg = String.format("%-1s", processedUsageFlg);

        // 处理 pi_csm
        String processedCsm = "";
        if (Math.signum(piCsm) == -1) {
            processedCsm = "-";
        } else {
            processedCsm = "+";
        }
        processedCsm += String.format("%018d", (long) (Math.abs(piCsm) * 1000000));

        // 处理 pi_mtrrec_UOM_CD
        String processedUomCd = piMtrrecUOCD != null ? piMtrrecUOCD : "";
        processedUomCd = String.format("%-4s", processedUomCd);

        // 处理 pi_tot_csm
        String processedTotCsm = "";
        if (Math.signum(piTotCsm) == -1) {
            processedTotCsm = "-";
        } else {
            processedTotCsm = "+";
        }
        processedTotCsm += String.format("%018d", (long) (Math.abs(piTotCsm) * 1000000));

        String vOutput = processedMeterNo + processedStartReadDt + processedStartReading + processedStartReadType +
                processedEndReadDt + processedEndReading + processedEndReadType + processedUsageFlg +
                processedCsm + processedUomCd + processedTotCsm+ accountId;

        return vOutput;
    }

    //
    public static String ctDetailForPrinterKey3000(BigDecimal piBalanceBf, BigDecimal piTotalCurCharge,
                                                   BigDecimal piDepositOffset, BigDecimal piBalanceCf,
                                                   BigDecimal piAmountDue) {
        String vOutput = "";
        // 处理 pi_balance_bf
        vOutput += (piBalanceBf.signum() == -1 ? "-" : "+") + String.format("%015d", (long) (Math.abs(piBalanceBf.multiply(BigDecimal.valueOf(100)).longValue())));
        // 处理 pi_total_cur_charge
        vOutput += (piTotalCurCharge.signum() == -1 ? "-" : "+") + String.format("%015d", (long) (Math.abs(piTotalCurCharge.multiply(BigDecimal.valueOf(100)).longValue())));
        // 处理 pi_deposit_offset
        vOutput += (piDepositOffset.signum() == -1 ? "-" : "+") + String.format("%015d", (long) (Math.abs(piDepositOffset.multiply(BigDecimal.valueOf(100)).longValue())));
        // 处理 pi_balance_cf
        vOutput += (piBalanceCf.signum() == -1 ? "-" : "+") + String.format("%015d", (long) (Math.abs(piBalanceCf.multiply(BigDecimal.valueOf(100)).longValue())));
        // 处理 pi_amount_due
        vOutput += (piAmountDue.signum() == -1 ? "-" : "+") + String.format("%015d", (long) (Math.abs(piAmountDue.multiply(BigDecimal.valueOf(100)).longValue())));

        return vOutput.trim();
    }

    /**
     * 执行消息替换操作
     *
     * @param piMsgOnBill2 要进行替换操作的原始字符串
     * @param piMsgOnBill1 用于替换的字符串
     * @return 替换后的字符串
     */
    public static String zs103SetupSqlSMsgRepl(String piMsgOnBill2, String piMsgOnBill1) {
        // 对piMsgOnBill1进行去空格操作
        String trimmedPiMsgOnBill1 = piMsgOnBill1.trim();
        // 进行替换操作
        String poMsgOnBill = piMsgOnBill2.replace("%1", trimmedPiMsgOnBill1);
        return poMsgOnBill;
    }

    /**
     * 执行消息替换操作
     *
     * @param piMsgOnBill5 待替换的原始消息字符串
     * @param piMsgOnBill1 用于替换 %1 的字符串
     * @param piMsgOnBill2 用于替换 %2 的字符串
     * @param piMsgOnBill3 用于替换 %3 的字符串
     * @param piMsgOnBill4 用于替换 %4 的字符串
     * @return 替换后的消息字符串
     */
    public static String zs104SetupSqlSSbdmRepl(String piMsgOnBill5, String piMsgOnBill1, String piMsgOnBill2, String piMsgOnBill3, String piMsgOnBill4) {
        // 对输入的替换字符串去除首尾空格
        String trimmedPiMsgOnBill1 = piMsgOnBill1.trim();
        String trimmedPiMsgOnBill2 = piMsgOnBill2.trim();
        String trimmedPiMsgOnBill3 = piMsgOnBill3.trim();
        String trimmedPiMsgOnBill4 = piMsgOnBill4.trim();

        // 依次进行替换操作
        String result = piMsgOnBill5.replace("%1", trimmedPiMsgOnBill1);
        result = result.replace("%2", trimmedPiMsgOnBill2);
        result = result.replace("%3", trimmedPiMsgOnBill3);
        result = result.replace("%4", trimmedPiMsgOnBill4);

        return result;
    }

    /**
     * 实现与 Oracle 函数 ct_detail_for_printer_key_3200 类似的功能
     *
     * @param piSvcId          服务 ID
     * @param piBitemId        项目 ID
     * @param piHdrSeqNo       头部序列号
     * @param piStartDt        开始日期
     * @param piEndDt          结束日期
     * @param piPrintDtSw      打印日期标志
     * @param piBitemCalcDescr 项目计算描述
     * @param piBitemCalcAmt   项目计算金额
     * @return 拼接并格式化后的字符串
     */
    public static String ctDetailForPrinterKey3200(String piSvcId, String piBitemId, Integer piHdrSeqNo,
                                                   String piStartDt, String piEndDt, String piPrintDtSw,
                                                   String piBitemCalcDescr, BigDecimal piBitemCalcAmt) {
        // 拼接字符串，模拟 rpad 和 lpad 操作
        String vOutput = String.format("%-10s%-12s%03d%-10s%-10s%-1s%-80.80s%s%015d",
                piSvcId != null ? piSvcId : " ",
                piBitemId != null ? piBitemId : " ",
                piHdrSeqNo != null ? Math.abs(piHdrSeqNo) : 0,
                piStartDt != null ? piStartDt : " ",
                piEndDt != null ? piEndDt : " ",
                piPrintDtSw != null ? piPrintDtSw : " ",
                piBitemCalcDescr != null ? piBitemCalcDescr : " ",
                piBitemCalcAmt != null && piBitemCalcAmt.compareTo(BigDecimal.ZERO) >= 0 ? "+" : "-",
                piBitemCalcAmt != null ? piBitemCalcAmt.abs().multiply(new BigDecimal("100")).setScale(0, RoundingMode.DOWN).intValue() : 0
        );
        return vOutput.trim();
    }

    public static String ctDetailForPrinterKey3210(String piBitemId, int piHdrSeqNo, String piSeqNo, String piDescrOnBill, double piCalcAmt) {
        String vOutput;
        // 处理 piBitemId
        String processedBitemId = piBitemId != null ? piBitemId : " ";
        processedBitemId = String.format("%-12s", processedBitemId);

        // 处理 piHdrSeqNo
        int absHdrSeqNo = Math.abs(piHdrSeqNo);
        String processedHdrSeqNo = String.format("%03d", absHdrSeqNo);

        // 处理 piSeqNo
        String processedSeqNo = piSeqNo != null ? piSeqNo : " ";
        processedSeqNo = String.format("%-3s", processedSeqNo);

        // 处理 piDescrOnBill
        String processedDescrOnBill = piDescrOnBill != null ? piDescrOnBill : " ";
        String paddedDescr = processedDescrOnBill + String.format("%-80s", "").substring(0, 80);
        processedDescrOnBill = paddedDescr.substring(0, 80);

        // 处理 piCalcAmt
        double absCalcAmt = Math.abs(piCalcAmt);
        String sign = Math.signum(piCalcAmt) == -1 ? "-" : "+";
        String processedCalcAmt = String.format("%015d", (long) (BigDecimal.valueOf(absCalcAmt).setScale(2, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100))).intValue());
        vOutput = processedBitemId + processedHdrSeqNo + processedSeqNo + processedDescrOnBill + sign + processedCalcAmt;
        return vOutput.trim();
    }


    public static String ctDetailForPrinterKey3300(String piDescrOnBill, Double piCurAmt) {
        String vOutput;
        // 处理 piDescrOnBill
        String processedDescrOnBill = piDescrOnBill != null ? piDescrOnBill : " ";
        String paddedDescr = processedDescrOnBill + String.format("%-80s", "").substring(0, 80);
        processedDescrOnBill = paddedDescr.substring(0, 80);

        // 处理 piCurAmt
        double absCurAmt = Math.abs((piCurAmt != null ? piCurAmt : 0) * 100);
        String sign = Math.signum(piCurAmt) == -1 ? "-" : "+";
        String processedCurAmt = String.format("%015d", (long) BigDecimal.valueOf(absCurAmt).setScale(0, RoundingMode.HALF_UP).longValue());

        vOutput = processedDescrOnBill + sign + processedCurAmt;

        return vOutput.trim();
    }

    public static String ctDetailForPrinterKey3100(String piSvcId, String piSdDescr, Double piSdSubtotal, String piSubtotalPrintSw, String piDsdChargesSw) {
        String vOutput;

        // 处理 piSvcId
        String processedSvcId = nvl(piSvcId, " ");
        processedSvcId = String.format("%-10s", processedSvcId);

        // 处理 piSdDescr
        String processedSdDescr = nvl(piSdDescr, " ");
        String paddedDescr = processedSdDescr + String.format("%-80s", "").substring(0, 80);
        processedSdDescr = paddedDescr.substring(0, 80);

        // 处理 piSdSubtotal
        double absSdSubtotal = Math.abs(nvl(piSdSubtotal, 0));
        String sign = sign(piSdSubtotal) == -1 ? "-" : "+";
        String processedSdSubtotal = String.format("%015d", (long) (BigDecimal.valueOf(absSdSubtotal).setScale(2, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100))).intValue());

        // 处理 piSubtotalPrintSw
        String processedSubtotalPrintSw = nvl(piSubtotalPrintSw, " ");
        processedSubtotalPrintSw = String.format("%-1s", processedSubtotalPrintSw);

        // 处理 piDsdChargesSw
        String processedDsdChargesSw = nvl(piDsdChargesSw, " ");
        processedDsdChargesSw = String.format("%-1s", processedDsdChargesSw);

        vOutput = processedSvcId + processedSdDescr + sign + processedSdSubtotal + processedSubtotalPrintSw + processedDsdChargesSw;

        return vOutput.trim();
    }

    public static String ctDetailForPrinterKey2200(String piMonthYy, String piReadType, double piAvgDailyCsm) {
        String vOutput;

        // 处理 pi_month_yy
        String processedMonthYy = nvl(piMonthYy, " ");
        processedMonthYy = String.format("%-5s", processedMonthYy);

        // 处理 pi_read_type
        String processedReadType = nvl(piReadType, " ");
        processedReadType = String.format("%-1s", processedReadType);

        // 处理 pi_avg_daily_csm
        double absAvgDailyCsm = Math.abs(nvl(piAvgDailyCsm, 0) * 1000000);
        String sign = sign(piAvgDailyCsm) == -1 ? "-" : "+";
        String processedAvgDailyCsm = String.format("%015d", (long) absAvgDailyCsm);

        vOutput = processedMonthYy + processedReadType + sign + processedAvgDailyCsm;

        return vOutput.trim();
    }

    // 实现 ct_detail_for_printer_key_2300 函数的功能
    public static String ctDetailForPrinterKey2300(int piCsmDays, double piGallons, double piCubicMeters, double piAvgDailyCsm, String piAdcSw, double piAdcLiter) {
        String vOutput;
        String vCheckPoint = "0000";

        // 处理 pi_csm_days
        String processedCsmDays = String.format("%04d", (int) Math.abs(nvl((double) piCsmDays, 0)));

        // 处理 pi_gallons
        String signGallons = sign(piGallons) == -1 ? "-" : "+";
        String processedGallons = signGallons + String.format("%018d", (long) (Math.abs(nvl(piGallons, 0)) * 1000000));

        // 处理 pi_cubic_meters
        String signCubicMeters = sign(piCubicMeters) == -1 ? "-" : "+";
        String processedCubicMeters = signCubicMeters + String.format("%018d", (long) (Math.abs(nvl(piCubicMeters, 0)) * 1000000));

        // 处理 pi_avg_daily_csm
        String signAvgDailyCsm = sign(piAvgDailyCsm) == -1 ? "-" : "+";
        String processedAvgDailyCsm = signAvgDailyCsm + String.format("%015d", (long) (Math.abs(nvl(piAvgDailyCsm, 0)) * 1000000));

        // 处理 pi_adc_sw
        String processedAdcSw = nvl(piAdcSw, " ");
        processedAdcSw = String.format("%-1s", processedAdcSw);

        // 处理 pi_adc_liter
        String signAdcLiter = sign(piAdcLiter) == -1 ? "-" : "+";
        String processedAdcLiter = signAdcLiter + String.format("%015d", (long) (Math.abs(nvl(piAdcLiter, 0)) * 1000000));

        vOutput = processedCsmDays + processedGallons + processedCubicMeters + processedAvgDailyCsm + processedAdcSw + processedAdcLiter;

        return vOutput.trim();
    }

    // 实现 ZZ000_SETUP_SQL_I_CI_MSG 存储过程的功能
    public void zz000SetupSqlICiMsg(String piBillId, String piMsgSeverityFlg, String piCallSeq) {
        GvGlobalVariableBatch gvGlobalVariableBatch1 = gvGlobalVariableBatch.get();
        // 增加 w_msg_log_cnt 的值
        gvGlobalVariableBatch1.setW_msg_log_cnt(gvGlobalVariableBatch1.getW_msg_log_cnt() + 1);
        // 拼接并截取 gv_debug_msg
        gvGlobalVariableBatch1.setGv_debug_msg(gvGlobalVariableBatch1.getGv_debug_msg() + ";" + piCallSeq);
        if (gvGlobalVariableBatch1.getGv_debug_msg().length() > 2200) {
            gvGlobalVariableBatch1.setGv_debug_msg(gvGlobalVariableBatch1.getGv_debug_msg().substring(0, 2200));
        }
    }

    // 模拟 nvl 函数处理字符串
    public static String nvl(String value, String defaultValue) {
        return value != null ? value : defaultValue;
    }

    // 模拟 nvl 函数处理数字
    public static double nvl(Double value, double defaultValue) {
        return value != null ? value : defaultValue;
    }

    // 模拟 sign 函数
    public static int sign(double num) {
        return (int) Math.signum(num);
    }

    // 模拟 trim 方法
    public static String trim(String str) {
        return str == null ? null : str.trim();
    }

    public static boolean existBxFinTran(String bitemId, List<FinTranWithAdj> finTranWithAdjList) {
        for (FinTranWithAdj finTran : finTranWithAdjList) {
            if ("BX".equals(finTran.getFinTranTypeInd()) && bitemId.equals(finTran.getFinTranTypeId())) {
                return true;
            }
        }
        return false;
    }

    public static  boolean existFinTranWithPayment(PaymentWithDtl payment, List<FinTranDto> FinTranDtoList, String finTranTypeInd) {
        for (FinTranDto finTran : FinTranDtoList) {
            if (finTranTypeInd.equals(finTran.getFinTranTypeInd()) && payment.getPaymentDtlId().equals(finTran.getRelatedId())) {
                return true;
            }
        }
        return false;
    }

    public static boolean in(String val, String... vals) {
        if (vals == null || vals.length == 0) {
            return false;
        }
        for (String v : vals) {
            if (v.equals(val)) {
                return true;
            }
        }
        return false;
    }

    public static boolean or(boolean... conds) {
        for (boolean cond : conds) {
            if (cond) {
                return true;
            }
        }
        return false;
    }

    public static boolean and(boolean... conds) {
        for (boolean cond : conds) {
            if (!cond) {
                return false;
            }
        }
        return true;
    }

    public static boolean premiseEquals(String premiseId1, String premiseId2) {
        if (Strings.isBlank(premiseId1)) {
          return Strings.isBlank(premiseId2);
        }
        if (Strings.isBlank(premiseId2)) {
            return false;
        }
        return premiseId1.equals(premiseId2);
    }
}
