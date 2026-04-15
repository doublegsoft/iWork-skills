package io.doublegsoft.usebase.aggregate;

import com.doublegsoft.jcommons.metabean.AttributeDefinition;
import com.doublegsoft.jcommons.metabean.ObjectDefinition;

import java.util.*;

public class AggregateRelationshipChain {

  private final ObjectDefinition aggregate;

  private final Map<String, AttributeRelationships> indexedRelationships = new HashMap<>();

  private final List<Relationship> orderingRelationships = new ArrayList<>();

  private final List<ObjectDefinition> declaringObjects = new ArrayList<>();

  private final Map<String, Boolean> objectArrays = new HashMap<>();

  public AggregateRelationshipChain(ObjectDefinition object) {
    this.aggregate = object;
  }

  public void addRelationship(Relationship rel) {
    AttributeRelationships attrRels = indexedRelationships.get(rel.toString());
    if (attrRels == null) {
      attrRels = new AttributeRelationships(rel.getSourceAttribute());
      indexedRelationships.put(rel.toString(), attrRels);
    }
    attrRels.addRelation(rel);
    orderingRelationships.add(rel);
  }

  public void addObject(ObjectDefinition obj) {
    addObject(obj, false);
  }

  public void addObject(ObjectDefinition obj, boolean isArray) {
    declaringObjects.add(obj);
    objectArrays.put(obj.getName(), isArray);
  }

  public List<ObjectDefinition> getObjects() {
    return declaringObjects;
  }

  public ObjectDefinition getObject(String objname) {
    for (ObjectDefinition obj : declaringObjects) {
      if (obj.getName().equals(objname)) {
        return obj;
      }
    }
    return null;
  }

  public Relationship getRelationship(String sourceObjName, String targetObjName) {
    for (AttributeRelationships rels : indexedRelationships.values()) {
      for (Relationship rel : rels.getRelationships()) {
        AttributeDefinition sourceAttr = rel.getSourceAttribute();
        AttributeDefinition targetAttr = rel.getTargetAttribute();
        if (sourceAttr.getParent().getName().equals(sourceObjName) &&
            targetAttr.getParent().getName().equals(targetObjName)) {
          return rel;
        }
        if (sourceAttr.getParent().getName().equals(targetObjName) &&
            targetAttr.getParent().getName().equals(sourceObjName)) {
          return rel;
        }
      }
    }
    return null;
  }

  public List<Relationship> getRelationships() {
    List<Relationship> retVal = new ArrayList<>();
    for (AttributeRelationships rels : indexedRelationships.values()) {
      retVal.addAll(rels.getRelationships());
    }
    return retVal;
  }

  public List<Relationship> getRelationships(String sourceObjName) {
    List<Relationship> retVal = new ArrayList<>();
    for (AttributeRelationships rels : indexedRelationships.values()) {
      for (Relationship rel : rels.getRelationships()) {
        AttributeDefinition sourceAttr = rel.getSourceAttribute();
        AttributeDefinition targetAttr = rel.getTargetAttribute();
        if (sourceAttr.getParent().getName().equals(sourceObjName)) {
          retVal.add(rel);
        }
        if (targetAttr.getParent().getName().equals(sourceObjName)) {
          retVal.add(rel);
        }
      }
    }
    return retVal;
  }

  public ObjectRelationships getStrongObjectRelationships(String objName) {
    List<ObjectRelationships> objRelsList = build();
    for (ObjectRelationships objRels : objRelsList) {
      if (objRels.getObject().getName().equals(objName)) {
        continue;
      }
      for (Relationship rel : objRels.getRelationships()) {
        if (objName.equals(rel.getSourceObject().getName()) ||
            objName.equals(rel.getTargetObject().getName())) {
          return objRels;
        }
      }
    }
    return null;
  }

  public List<ObjectRelationships> build() {
    if (declaringObjects.isEmpty()) {
      return Collections.emptyList();
    }
    return build(declaringObjects.get(0));
  }

  public List<ObjectRelationships> build(String rootObjName) {
    List<ObjectRelationships> retVal = new ArrayList<>();
    ObjectDefinition rootObj = getObject(rootObjName);
    if (rootObj == null) {
      throw new IllegalArgumentException("not found \"" + rootObjName + "\" in chain");
    }
    return build(rootObj);
  }

  public List<ObjectRelationships> build(ObjectDefinition obj) {
    List<ObjectRelationships> retVal = new ArrayList<>();
    Map<String, ObjectRelationships> processedObjs = new HashMap<>();
    build(obj, processedObjs, retVal);
    return retVal;
  }

  private void build(ObjectDefinition rootObj,
                     Map<String, ObjectRelationships> processedObjs,
                     List<ObjectRelationships> returnedObjs) {
    ObjectRelationships objRels = processedObjs.get(rootObj.getName());
    if (objRels != null) {
      return;
    }
    objRels = new ObjectRelationships(rootObj);
    Boolean flag = objectArrays.get(rootObj.getName());
    objRels.setArray(flag != null && flag);
    processedObjs.put(rootObj.getName(), objRels);
    returnedObjs.add(objRels);
    for (Relationship rel : orderingRelationships) {
      if (rel.getSourceObject().equals(rootObj)) {
        objRels.addRelationship(rel);
        build(rel.getTargetObject(), processedObjs, returnedObjs);
      } else if (rel.getTargetObject().equals(rootObj)) {
        objRels.addRelationship(rel);
        build(rel.getSourceObject(), processedObjs, returnedObjs);
      }
    }
  }
}
