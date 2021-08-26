/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.ResolveHints.SkewedJoin
import org.apache.spark.sql.catalyst.expressions.{Ascending, Cast, Expression, GreaterThan, GreaterThanOrEqual, In, IntegerLiteral, LessThan, LessThanOrEqual, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType


/**
 * Collection of rules related to hints. The only hint currently available is join strategy hint.
 *
 * Note that this is separately into two rules because in the future we might introduce new hint
 * rules that have different ordering requirements from join strategies.
 */
object ResolveHints {

  /**
   * The list of allowed join strategy hints is defined in [[JoinStrategyHint.strategies]], and a
   * sequence of relation aliases can be specified with a join strategy hint, e.g., "MERGE(a, c)",
   * "BROADCAST(a)". A join strategy hint plan node will be inserted on top of any relation (that
   * is not aliased differently), subquery, or common table expression that match the specified
   * name.
   *
   * The hint resolution works by recursively traversing down the query plan to find a relation or
   * subquery that matches one of the specified relation aliases. The traversal does not go past
   * beyond any view reference, with clause or subquery alias.
   *
   * This rule must happen before common table expressions.
   */
  class ResolveJoinStrategyHints(conf: SQLConf) extends Rule[LogicalPlan] {
    private val STRATEGY_HINT_NAMES = JoinStrategyHint.strategies.flatMap(_.hintAliases)

    private val hintErrorHandler = conf.hintErrorHandler

    // fix hint method bug and add some operation,DOUBLE_TYPE and SKEWED_JOIN
    // SKEWED_JOIN(join_key(left.field, right.field), skewed_values('value1', 'value2'))
    private val SKEWED_JOIN = "SKEWED_JOIN"

    def resolver: Resolver = conf.resolver

    private def createHintInfo(hintName: String): HintInfo = {
      HintInfo(strategy =
        JoinStrategyHint.strategies.find(
          _.hintAliases.map(
            _.toUpperCase(Locale.ROOT)).contains(hintName.toUpperCase(Locale.ROOT))))
    }
    // This method checks if given multi-part identifiers are matched with each other.
    // The [[ResolveJoinStrategyHints]] rule is applied before the resolution batch
    // in the analyzer and we cannot semantically compare them at this stage.
    // Therefore, we follow a simple rule; they match if an identifier in a hint
    // is a tail of an identifier in a relation. This process is independent of a session
    // catalog (`currentDb` in [[SessionCatalog]]) and it just compares them literally.
    //
    // For example,
    //  * in a query `SELECT /*+ BROADCAST(t) */ * FROM db1.t JOIN t`,
    //    the broadcast hint will match both tables, `db1.t` and `t`,
    //    even when the current db is `db2`.
    //  * in a query `SELECT /*+ BROADCAST(default.t) */ * FROM default.t JOIN t`,
    //    the broadcast hint will match the left-side table only, `default.t`.
    private def matchedIdentifier(identInHint: Seq[String], identInQuery: Seq[String]): Boolean = {
      if (identInHint.length <= identInQuery.length) {
        identInHint.zip(identInQuery.takeRight(identInHint.length))
          .forall { case (i1, i2) => resolver(i1, i2) }
      } else {
        false
      }
    }

    private def extractIdentifier(r: SubqueryAlias): Seq[String] = {
      r.identifier.qualifier :+ r.identifier.name
    }

    private def applyJoinStrategyHint(
        plan: LogicalPlan,
        relationsInHint: Set[Seq[String]],
        relationsInHintWithMatch: mutable.HashSet[Seq[String]],
        hintName: String): LogicalPlan = {
      // Whether to continue recursing down the tree
      var recurse = true

      def matchedIdentifierInHint(identInQuery: Seq[String]): Boolean = {
        relationsInHint.find(matchedIdentifier(_, identInQuery))
          .map(relationsInHintWithMatch.add).nonEmpty
      }

      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case ResolvedHint(u @ UnresolvedRelation(ident), hint)
              if matchedIdentifierInHint(ident) =>
            ResolvedHint(u, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case ResolvedHint(r: SubqueryAlias, hint)
              if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(r, createHintInfo(hintName).merge(hint, hintErrorHandler))

          case UnresolvedRelation(ident) if matchedIdentifierInHint(ident) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case r: SubqueryAlias if matchedIdentifierInHint(extractIdentifier(r)) =>
            ResolvedHint(plan, createHintInfo(hintName))

          case _: ResolvedHint | _: View | _: With | _: SubqueryAlias =>
            // Don't traverse down these nodes.
            // For an existing strategy hint, there is no chance for a match from this point down.
            // The rest (view, with, subquery) indicates different scopes that we shouldn't traverse
            // down. Note that technically when this rule is executed, we haven't completed view
            // resolution yet and as a result the view part should be deadcode. I'm leaving it here
            // to be more future proof in case we change the view we do view resolution.
            recurse = false
            plan

          case _ =>
            plan
        }
      }

      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren { child =>
          applyJoinStrategyHint(child, relationsInHint, relationsInHintWithMatch, hintName)
        }
      } else {
        newNode
      }
    }

    // fix hint method bug and add some operation,DOUBLE_TYPE and SKEWED_JOIN
    private def applyCastHint(expression: Expression, castFieldId: Set[String]): Expression = {
      val newExpression = expression match {
        case attribute: UnresolvedAttribute if castFieldId.contains(attribute.name) =>
          Cast(attribute, DoubleType)
        case other => other
      }
      newExpression
    }

    private def getTbAlias(plan: LogicalPlan, tableName: String): String = {
      plan.map(lp => lp)
        .filter(_.isInstanceOf[SubqueryAlias])
        .map(_.asInstanceOf[SubqueryAlias])
        .filter(_.child.isInstanceOf[UnresolvedRelation])
        .filter{ sa =>
          sa.child.asInstanceOf[UnresolvedRelation].tableName == tableName
        }.map(s => s"${s.alias}.").headOption.getOrElse("")
    }

    private def applySkewedJoinHint(plan: LogicalPlan, skewedJoin: SkewedJoin): LogicalPlan = {
      // scalastyle:off println
      var recurse = true
      val newNode = CurrentOrigin.withOrigin(plan.origin) {
        plan match {
          case Join(left, right, joinType, condition, hint) if condition.isDefined =>
            val joinKey = skewedJoin.joinKey
            val hasLeftTb = left.find { lp =>
              lp.isInstanceOf[UnresolvedRelation] &&
                lp.asInstanceOf[UnresolvedRelation].tableName == joinKey.leftTable
            }.isDefined

            val hasRightTb = right.find { lp =>
              lp.isInstanceOf[UnresolvedRelation] &&
                lp.asInstanceOf[UnresolvedRelation].tableName == joinKey.rightTable
            }.isDefined

            val leftField = getTbAlias(left, joinKey.leftTable) + joinKey.leftField
            val rightField = getTbAlias(right, joinKey.rightTable) + joinKey.rightField
            val joinKeys = condition.get.map(expr => expr)
              .filter(_.isInstanceOf[UnresolvedAttribute])
              .map(_.asInstanceOf[UnresolvedAttribute].name)
              .filter(n => n.endsWith(joinKey.leftField) || n.endsWith(joinKey.rightField))

            val newPlan = if (hasLeftTb && hasRightTb && joinKeys.length >= 2) {
              val inList = skewedJoin.skewedValues.map(Literal(_))
              val left1 = Filter(Not(In(UnresolvedAttribute(leftField), inList)), left)
              val right1 = Filter(Not(In(UnresolvedAttribute(rightField), inList)), right)
              val left2 = Filter(In(UnresolvedAttribute(leftField), inList), left)
              val right2 = Filter(In(UnresolvedAttribute(rightField), inList), right)

              val join1 = Join(left1, right1, joinType, condition, hint)
              // use mapjoin
              val join2 = Join(ResolvedHint(left2, HintInfo()), ResolvedHint(right2, HintInfo()), Inner, condition, hint)
              Union(Seq(join1, join2))
            } else plan
            ResolvedHint(newPlan)
          case _: ResolvedHint | _: View | _: With | _: SubqueryAlias =>
            recurse = false
            plan

          case _ =>
            plan
        }
      }
      if ((plan fastEquals newNode) && recurse) {
        newNode.mapChildren(child => applySkewedJoinHint(child, skewedJoin))
      } else {
        newNode
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = {
      var castFieldId: Set[String] = Set()

      val newNode = plan transformUp {
        case h: UnresolvedHint if STRATEGY_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
          if (h.parameters.isEmpty) {
            // If there is no table alias specified, apply the hint on the entire subtree.
            ResolvedHint(h.child, createHintInfo(h.name))
          } else {
            // Otherwise, find within the subtree query plans to apply the hint.
            val relationNamesInHint = h.parameters.map {
              case tableName: String => UnresolvedAttribute.parseAttributeName(tableName)
              case tableId: UnresolvedAttribute => tableId.nameParts
              case unsupported => throw new AnalysisException("Join strategy hint parameter " +
                s"should be an identifier or string but was $unsupported (${unsupported.getClass}")
            }.toSet
            val relationsInHintWithMatch = new mutable.HashSet[Seq[String]]
            val applied = applyJoinStrategyHint(
              h.child, relationNamesInHint, relationsInHintWithMatch, h.name)

            // Filters unmatched relation identifiers in the hint
            val unmatchedIdents = relationNamesInHint -- relationsInHintWithMatch
            hintErrorHandler.hintRelationsNotFound(h.name, h.parameters, unmatchedIdents)
            applied
          }

        case d: UnresolvedHint if STRATEGY_HINT_NAMES.contains(d.name.toUpperCase(Locale.ROOT)) =>
          if (d.parameters.isEmpty) {
            ResolvedHint(d.child, createHintInfo(d.name))
          } else {
            castFieldId ++= d.parameters.map {
              case fieldId: String => fieldId
              case fieldId: UnresolvedAttribute => fieldId.name
              case unsupported => throw new AnalysisException("CAST hint parameter should be" +
                s" string but was $unsupported (${unsupported.getClass}")
            }.toSet
            ResolvedHint(d.child, createHintInfo(d.name))
          }

        case h: UnresolvedHint if SKEWED_JOIN == h.name.toUpperCase(Locale.ROOT) =>
          val paramMap = h.parameters.map {
            case UnresolvedFunction(funId, children, _, _) =>
              (funId.funcName, children.map {
                case ua: UnresolvedAttribute => ua.name
                case other => other.toString
              })
            case unsupported => throw new AnalysisException("SKEWED hint parameter should be" +
              s" Function but was $unsupported (${unsupported.getClass}")
          }.toMap
          val joinKey = paramMap.get("join_key")
          val skewedValues = paramMap.get("skewed_values")
          if (joinKey.nonEmpty && joinKey.get.length == 2
            && skewedValues.nonEmpty && skewedValues.get.length > 0) {
            applySkewedJoinHint(h.child,
              SkewedJoin(JoinKey(joinKey.get(0), joinKey.get(1)), skewedValues.get))
          } else {
            ResolvedHint(h.child)
          }
      }

      if (castFieldId.isEmpty) {
        newNode
      } else {
        plan transformExpressions  {
          case SortOrder(child, direction, nullOrdering, sameOrderExpressions) =>
            SortOrder(applyCastHint(child, castFieldId),
              direction, nullOrdering, sameOrderExpressions)

          case GreaterThan(left, right) =>
            GreaterThan(applyCastHint(left, castFieldId), applyCastHint(right, castFieldId))

          case GreaterThanOrEqual(left, right) =>
            GreaterThanOrEqual(applyCastHint(left, castFieldId), applyCastHint(right, castFieldId))

          case LessThan(left, right) =>
            LessThan(applyCastHint(left, castFieldId), applyCastHint(right, castFieldId))

          case LessThanOrEqual(left, right) =>
            LessThanOrEqual(applyCastHint(left, castFieldId), applyCastHint(right, castFieldId))
        }
      }
    }
  }

  /**
   * COALESCE Hint accepts names "COALESCE", "REPARTITION", and "REPARTITION_BY_RANGE".
   */
  class ResolveCoalesceHints(conf: SQLConf) extends Rule[LogicalPlan] {

    /**
     * This function handles hints for "COALESCE" and "REPARTITION".
     * The "COALESCE" hint only has a partition number as a parameter. The "REPARTITION" hint
     * has a partition number, columns, or both of them as parameters.
     */
    private def createRepartition(
        shuffle: Boolean, hint: UnresolvedHint): LogicalPlan = {
      val hintName = hint.name.toUpperCase(Locale.ROOT)

      def createRepartitionByExpression(
          numPartitions: Int, partitionExprs: Seq[Any]): RepartitionByExpression = {
        val sortOrders = partitionExprs.filter(_.isInstanceOf[SortOrder])
        if (sortOrders.nonEmpty) throw new IllegalArgumentException(
          s"""Invalid partitionExprs specified: $sortOrders
             |For range partitioning use REPARTITION_BY_RANGE instead.
           """.stripMargin)
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          throw new AnalysisException(s"$hintName Hint parameter should include columns, but " +
            s"${invalidParams.mkString(", ")} found")
        }
        RepartitionByExpression(
          partitionExprs.map(_.asInstanceOf[Expression]), hint.child, numPartitions)
      }

      hint.parameters match {
        case Seq(IntegerLiteral(numPartitions)) =>
          Repartition(numPartitions, shuffle, hint.child)
        case Seq(numPartitions: Int) =>
          Repartition(numPartitions, shuffle, hint.child)
        // The "COALESCE" hint (shuffle = false) must have a partition number only
        case _ if !shuffle =>
          throw new AnalysisException(s"$hintName Hint expects a partition number as a parameter")

        case param @ Seq(IntegerLiteral(numPartitions), _*) if shuffle =>
          createRepartitionByExpression(numPartitions, param.tail)
        case param @ Seq(numPartitions: Int, _*) if shuffle =>
          createRepartitionByExpression(numPartitions, param.tail)
        case param @ Seq(_*) if shuffle =>
          createRepartitionByExpression(conf.numShufflePartitions, param)
      }
    }

    /**
     * This function handles hints for "REPARTITION_BY_RANGE".
     * The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional.
     */
    private def createRepartitionByRange(hint: UnresolvedHint): RepartitionByExpression = {
      val hintName = hint.name.toUpperCase(Locale.ROOT)

      def createRepartitionByExpression(
          numPartitions: Int, partitionExprs: Seq[Any]): RepartitionByExpression = {
        val invalidParams = partitionExprs.filter(!_.isInstanceOf[UnresolvedAttribute])
        if (invalidParams.nonEmpty) {
          throw new AnalysisException(s"$hintName Hint parameter should include columns, but " +
            s"${invalidParams.mkString(", ")} found")
        }
        val sortOrder = partitionExprs.map {
          case expr: SortOrder => expr
          case expr: Expression => SortOrder(expr, Ascending)
        }
        RepartitionByExpression(sortOrder, hint.child, numPartitions)
      }

      hint.parameters match {
        case param @ Seq(IntegerLiteral(numPartitions), _*) =>
          createRepartitionByExpression(numPartitions, param.tail)
        case param @ Seq(numPartitions: Int, _*) =>
          createRepartitionByExpression(numPartitions, param.tail)
        case param @ Seq(_*) =>
          createRepartitionByExpression(conf.numShufflePartitions, param)
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case hint @ UnresolvedHint(hintName, _, _) => hintName.toUpperCase(Locale.ROOT) match {
          case "REPARTITION" =>
            createRepartition(shuffle = true, hint)
          case "COALESCE" =>
            createRepartition(shuffle = false, hint)
          case "REPARTITION_BY_RANGE" =>
            createRepartitionByRange(hint)
          case _ => hint
        }
    }
  }

  object ResolveCoalesceHints {
    val COALESCE_HINT_NAMES: Set[String] = Set("COALESCE", "REPARTITION", "REPARTITION_BY_RANGE")
  }

  /**
   * Removes all the hints, used to remove invalid hints provided by the user.
   * This must be executed after all the other hint rules are executed.
   */
  class RemoveAllHints(conf: SQLConf) extends Rule[LogicalPlan] {

    private val hintErrorHandler = conf.hintErrorHandler

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
      case h: UnresolvedHint =>
        hintErrorHandler.hintNotRecognized(h.name, h.parameters)
        h.child
    }
  }

  case class JoinKey(leftTbField: String, rightTbField: String) {
    val leftTable: String = leftTbField.split("\\.")(0)
    val leftField: String = leftTbField.split("\\.")(1)
    val rightTable: String = rightTbField.split("\\.")(0)
    val rightField: String = rightTbField.split("\\.")(1)
  }
  case class SkewedJoin(joinKey: JoinKey, skewedValues: Seq[String])
}
