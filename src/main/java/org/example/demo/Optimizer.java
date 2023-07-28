package org.example.demo;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.Collections;
import java.util.Properties;

public class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;

    public Optimizer(
            CalciteConnectionConfig config,
            SqlValidator validator,
            SqlToRelConverter converter,
            VolcanoPlanner planner) {
        this.config = config;
        this.validator = validator;
        this.converter = converter;
        this.planner = planner;
    }

    public static Optimizer create(SimpleSchema schema) {
        Properties configProperties = new Properties();
        // 是否是大小写敏感
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        // Calcite中，UNQUOTED_CASING和QUOTED_CASING是用来指定标识符（如表名、列名、函数名等）的大小写规则的选项。
        // UNQUOTED_CASING：表示未加引号的标识符应该采用的大小写规则。这个选项有三种取值：
        // UNCHANGED：表示标识符大小写不变，例如表名为“employee”，在查询中使用“employee”或“EMPLOYEE”都是合法的。
        //   TO_UPPER：表示标识符应该全部转换为大写字母，例如表名为“employee”，在查询中只能使用“EMPLOYEE”。
        //   TO_LOWER：表示标识符应该全部转换为小写字母，例如表名为“Employee”，在查询中只能使用“employee”。
        //  QUOTED_CASING：表示加了引号的标识符应该采用的大小写规则。这个选项也有三种取值，与UNQUOTED_CASING相同。
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        // create root schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        // create catalog reader, needed by SqlValidator
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(schema.getSchemaName()),
                typeFactory,
                config);

        // create SqlValidator
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                // 是否允许在查询中使用模糊匹配的运算符
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                // 指定Sql的兼容性规范
                .withSqlConformance(config.conformance())
                // 指定空值的排序规则
                .withDefaultNullCollation(config.defaultNullCollation())
                // 是否启用标识符扩展
                .withIdentifierExpansion(true);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory, validatorConfig);

        // create VolcanoPlanner, needed by SqlToRelConverter and optimizer
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        // create SqlToRelConverter
        // RelOptCluster是一个逻辑计划的上下文，主要包含系统信息(字符集、时区)、全局配置(SqlConformance)、数据类型工厂等
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                // 是否在逻辑计划中移除未使用的列
                .withTrimUnusedFields(true)
                // 是否启用子查询展开,默认为false
                .withExpand(false);
        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return new Optimizer(config, validator, converter, planner);
    }

    public SqlNode parse(String sql) throws Exception {
        SqlParser.Config parserConfig = SqlParser.config()
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuoting(config.quoting())
                .withConformance(config.conformance())
                .withCaseSensitive(config.caseSensitive());
        SqlParser parser = SqlParser.create(sql, parserConfig);

        return parser.parseStmt();
    }

    public SqlNode validate(SqlNode node) {
        return validator.validate(node);
    }

    public RelNode convert(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);

        return root.rel;
    }

    public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        Program program = Programs.of(RuleSets.ofList(rules));

        return program.run(
                planner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList());
    }
}
