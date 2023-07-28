package org.example.demo

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable
import org.apache.calcite.adapter.enumerable.EnumerableRel
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.sql.dialect.MysqlSqlDialect
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.tools.RuleSets
import java.util.*
import java.util.stream.Collectors


@Suppress("INACCESSIBLE_TYPE")
fun main() {
    val sql = """ 
             SELECT u.id, name, age, sum(price) 
            FROM users AS u join orders AS o ON u.id = o.user_id 
            WHERE age >= 20 AND age <= 30 
            GROUP BY u.id, name, age 
            ORDER BY u.id """.trim()

    val curDirectoryPath = System.getProperty("user.dir")
    // 创建Schema, 一个Schema中包含多个表. Calcite中的Schema类似于RDBMS中的Database
    val userTable: SimpleTable = SimpleTable.newBuilder("users")
        .addField("id", SqlTypeName.VARCHAR)
        .addField("name", SqlTypeName.VARCHAR)
        .addField("age", SqlTypeName.INTEGER)
        .withFilePath("$curDirectoryPath/src/main/resources/user.csv")
        .withRowCount(10)
        .build()
    val orderTable: SimpleTable = SimpleTable.newBuilder("orders")
        .addField("id", SqlTypeName.VARCHAR)
        .addField("user_id", SqlTypeName.VARCHAR)
        .addField("goods", SqlTypeName.VARCHAR)
        .addField("price", SqlTypeName.DECIMAL)
        .withFilePath("$curDirectoryPath/src/main/resources/orders.csv")
        .withRowCount(10)
        .build()
    val schema: SimpleSchema = SimpleSchema.newBuilder("s")
        .addTable(userTable)
        .addTable(orderTable)
        .build()
    val rootSchema = CalciteSchema.createRootSchema(false, false)
    rootSchema.add(schema.getSchemaName(), schema)

    val optimizer: Optimizer = Optimizer.create(schema)

    // 1. SQL解析: string -> SqlNode
    val sqlNode = optimizer.parse(sql)
    printStep("origin sql", sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).toString())

    // 2. SQL校验: SqlNode -> SqlNode
    val validatedSqlNode = optimizer.validate(sqlNode)
    printStep("validated sql", sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).toString())

    // 3. SQL转换: SqlNode -> RelNode
    val relNode = optimizer.convert(validatedSqlNode)
    printStep("convert result", relNode.explain())

    // 4. SQL优化: RelNode -> RelNode
    val rules = RuleSets.ofList(
        CoreRules.FILTER_TO_CALC,
        CoreRules.PROJECT_TO_CALC,
        CoreRules.FILTER_CALC_MERGE,
        CoreRules.PROJECT_CALC_MERGE,
        CoreRules.FILTER_INTO_JOIN,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
        EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_CALC_RULE,
        EnumerableRules.ENUMERABLE_AGGREGATE_RULE
    )
    val optimizerRelTree = optimizer.optimize(
        relNode,
        relNode.traitSet.plus(EnumerableConvention.INSTANCE),
        rules
    )
    printStep("optimize result", optimizerRelTree.explain())

    // 5. SQL execute: RelNode --> 物理执行计划
    val enumerable = optimizerRelTree as EnumerableRel
    val internalParameters: Map<String, Any> = LinkedHashMap()
    val prefer = Prefer.ARRAY
    // 可枚举可解释节点转换为可绑定节点, 转换过程涉及到RelNode节点树的遍历和重建，以便将可枚举可解释节点转换为可绑定节点，并生成适当的执行计划。
    // 可绑定节点（Bindable）是一种RelNode节点类型，它是一种可以绑定到底层数据存储的节点
    val bindable = EnumerableInterpretable.toBindable(
        internalParameters,
        null, enumerable, prefer
    )
    // statement execute
    val bind = bindable.bind(SimpleDataContext(rootSchema.plus()))
    val enumerator = bind.enumerator()

    // 6. 输出执行结果
    printStep("execution result")
    while (enumerator.moveNext()) {
        val current = enumerator.current()
        val values = current as Array<*>
        val columnValues = values.asList()
            .stream().map {
                it.toString()
            }.collect(Collectors.joining(","))
        println(columnValues)
    }
}

fun printStep(step: String, content: String) {
    print("\n******************** $step ******************** \n")
    println(content)
}

fun printStep(step: String) {
    print("\n******************** $step ******************** \n")
}