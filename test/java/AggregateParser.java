package io.doublegsoft.usebase.parser;

import com.doublegsoft.jcommons.metabean.AttributeDefinition;
import com.doublegsoft.jcommons.metabean.ModelDefinition;
import com.doublegsoft.jcommons.metabean.ObjectDefinition;
import com.doublegsoft.jcommons.metabean.type.CollectionType;
import com.doublegsoft.jcommons.metabean.type.DomainType;
import com.doublegsoft.jcommons.metabean.type.PrimitiveType;
import com.doublegsoft.jcommons.metamodel.CalcExprDefinition;
import com.doublegsoft.jcommons.metamodel.ReturnedObjectDefinition;
import com.doublegsoft.jcommons.metamodel.UsecaseDefinition;
import com.doublegsoft.jcommons.utils.Inflector;
import io.doublegsoft.usebase.modelbase.ModelbaseHelper;

public class AggregateParser extends UsebaseParser {

  public AggregateParser(ModelDefinition dataModel) {
    super(dataModel);
  }

  public void assemble(io.doublegsoft.usebase.UsebaseParser.Usebase_aggregateContext ctx,
                       ObjectDefinition owner, UsecaseDefinition usecase) {
    for (int i = 0; i < ctx.usebase_data().size(); i++) {
      io.doublegsoft.usebase.UsebaseParser.Usebase_dataContext ctxData = ctx.usebase_data(i);
      if (ctxData.usebase_object() != null) {
        String originalObjName = ctxData.usebase_object().name.getText();
        io.doublegsoft.usebase.UsebaseParser.Usebase_objectContext ctxObj = ctxData.usebase_object();
        ModelbaseHelper.addOptions(owner, "original", "object", originalObjName);
        if (ctxObj.usebase_attributes() != null) {
          // 对象属性数据封装
          getAttributesParser().assemble(ctxObj.usebase_attributes(), owner, usecase);
          // 重新设置index和alias
          for (AttributeDefinition attrInOwner : owner.getAttributes()) {
            if (originalObjName.equals(attrInOwner.getLabelledOption("original", "object")) &&
                attrInOwner.getLabelledOption("original", "index") == null) {
              attrInOwner.setLabelledOption("original", "index", String.valueOf(i));
              if (ctxData.usebase_object().alias != null ) {
                attrInOwner.setLabelledOption("alias", "object", ctxData.usebase_object().alias.getText());
              }
            }
          }
        } else if (ctxObj.usebase_arguments() != null) {
          // 对象查询参数
          getArgumentsParser().decorate(ctxObj.usebase_arguments(), owner, usecase);
        } else {
          // 只有对象，为指定（选择）任何对象中的属性
          ObjectDefinition objInDataModel = dataModel.findObjectByName(ctxObj.name.getText());
          for (AttributeDefinition attrDef : objInDataModel.getAttributes()) {
            if (ModelbaseHelper.isSystemOrExistingInObject(attrDef.getName(), owner) || attrDef.getType().isCollection()) {
              continue;
            }
            AttributeDefinition attrInObj = ModelbaseHelper.cloneAttribute(attrDef, owner);
            if (ctxData.usebase_object().alias != null) {
              attrInObj.setLabelledOption("alias", "object", ctxData.usebase_object().alias.getText());
            }
            attrInObj.setLabelledOption("original", "index", String.valueOf(i));
          }
        }
        if (ctxData.usebase_object().usebase_source() != null) {
          getSourceParser().assemble(ctxData.usebase_object().usebase_source(), usecase, owner);
        }
        if (ctxData.usebase_object().usebase_arguments() != null) {
          getArgumentsParser().assembleOrCreateAndThen(ctxData.usebase_object().usebase_arguments(), owner, usecase);
          for (AttributeDefinition attr : owner.getAttributes()) {
            String strIndex = attr.getLabelledOption("original", "index");
            if (strIndex == null) {
              attr.setLabelledOption("original", "index", String.valueOf(i));
            }
            if (attr.getLabelledOption("original", "object") == null) {
              attr.setLabelledOption("original", "object", owner.getLabelledOption("original", "object"));
            }
            if (attr.getLabelledOption("original", "attribute") == null) {
              attr.setLabelledOption("original", "attribute", attr.getName());
            }
            if (ctxData.usebase_object().alias != null) {
              attr.setLabelledOption("alias", "object", ctxData.usebase_object().alias.getText());
            }
          }
        }
        if (ctxData.usebase_object().msg != null) {
          String msg = ctxData.usebase_object().msg.getText();
          msg = msg.substring(1, msg.length() - 1);
          owner.setLabelledOption("required", "message", msg);
        }
      } else if (ctxData.usebase_array() != null) {
        io.doublegsoft.usebase.UsebaseParser.Usebase_arrayContext ctxArr = ctxData.usebase_array();
        // 数组会额外产生内联对象
        String attrname = "";
        if (ctxArr.alias != null) {
          attrname = ctxArr.alias.getText();
        } else if (ctxArr.name != null){
          attrname = Inflector.getInstance().pluralize(ctxArr.name.getText());
        }
        if (ctxArr.name != null) {
          String objname = ctxData.usebase_array().name.getText();
          ObjectDefinition dummyArrayOwner = new ObjectDefinition("~" + objname, new ModelDefinition());
          getArrayParser().assemble(ctxArr, dummyArrayOwner, usecase);
          AttributeDefinition attrArray = new AttributeDefinition(attrname, owner);
          ObjectDefinition objInDataModel = dataModel.findObjectByName(objname);
          CollectionType colltype = new CollectionType("");
          colltype.setComponentType(objInDataModel);
          attrArray.setType(colltype);
          attrArray.setLabelledOption("original", "index", String.valueOf(i));
          if (attrArray.getLabelledOption("original", "object") == null) {
            attrArray.setLabelledOption("original", "object", objInDataModel.getName());
          }
        } else {
          getArrayParser().assemble(ctxArr, owner, usecase);
        }
        if (ctxArr.usebase_arguments() != null) {
          getArgumentsParser().decorate(ctxArr.usebase_arguments(), owner, usecase);
        }
        if (ctxData.usebase_array().msg != null) {
          String msg = ctxData.usebase_array().msg.getText();
          msg = msg.substring(1, msg.length() - 1);
          owner.setLabelledOption("required", "message", msg);
        }
        if (!(owner instanceof ReturnedObjectDefinition)) {
          owner.setLabelledOption("original", "array", "true");
        }
      } else if (ctxData.usebase_derivative() != null) {
        AttributeDefinition attrDeri = new AttributeDefinition(ctxData.usebase_derivative().name.getText(), owner);
        io.doublegsoft.usebase.UsebaseParser.Usebase_calculateContext ctxCalc = ctxData.usebase_derivative().usebase_calculate();
        if (ctxCalc != null) {
          if (ctxCalc.usebase_calc_expr() != null) {
            CalcExprDefinition calcExpr = new CalcExprDefinition();
            // TODO
            getCalcExprParser().assemble(ctxCalc.usebase_calc_expr(), calcExpr, usecase);
          } else if (ctxCalc.name != null && "count".equals(ctxCalc.name.getText())) {
            attrDeri.setType(new PrimitiveType("long"));
            attrDeri.getConstraint().setDomainType(new DomainType("long"));
            attrDeri.setLabelledOption("original", "operator", "count");
            if (ctxCalc.usebase_array() != null) {
              io.doublegsoft.usebase.UsebaseParser.Usebase_arrayContext ctxArr = ctxData.usebase_array();
              String objname = ctxCalc.usebase_array().usebase_aggregate().usebase_data(0).usebase_object().name.getText();
              attrDeri.setLabelledOption("original", "object", objname);
            }
          } else if (ctxCalc.name != null && "sum".equals(ctxCalc.name.getText())) {
            attrDeri.getConstraint().setDomainType(new DomainType("number"));
            PrimitiveType pt = new PrimitiveType("number");
            pt.setPrecision(12);
            pt.setScale(4);
            attrDeri.setType(pt);
          }
        } else {
          attrDeri.setType(new PrimitiveType("string"));
        }
        attrDeri.setLabelledOption("original", "index", String.valueOf(i));
      }
    }
    // 单独处理关联关系
    for (int i = 0; i < ctx.usebase_conditions().size(); i++) {
      io.doublegsoft.usebase.UsebaseParser.Usebase_conditionsContext ctxConds = ctx.usebase_conditions(i);
      getConditionsParser().assemble(ctxConds, owner, i);
    }
  }
}
