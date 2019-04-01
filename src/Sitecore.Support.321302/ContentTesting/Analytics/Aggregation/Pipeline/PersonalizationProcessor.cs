using System;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Analytics;
using Sitecore.Analytics.Aggregation.Pipeline;
using Sitecore.Analytics.Model;
using Sitecore.ContentTesting;
using Sitecore.ContentTesting.Analytics;
using Sitecore.ContentTesting.Analytics.Aggregation;
using Sitecore.ContentTesting.Analytics.Aggregation.Data.Model.Facts;
using Sitecore.ContentTesting.Configuration;
using Sitecore.ContentTesting.Model.Data.Items;
using Sitecore.ContentTesting.Models;
using Sitecore.Data;
using Sitecore.Diagnostics;
using Sitecore.Globalization;
using Version = Sitecore.Data.Version;

namespace Sitecore.Support.ContentTesting.Analytics.Aggregation.Pipeline
{
  public class PersonalizationProcessor : InteractionAggregationPipelineProcessor
  {
    private readonly TestPages _testPages;
    private readonly TestPageStatistics _testPageStatistics;

    public PersonalizationProcessor()
    {
      _testPages = new TestPages();
      _testPageStatistics = new TestPageStatistics();
    }

    public PersonalizationProcessor(TestPages testPages, TestPageStatistics testPageStatistics)
    {
      _testPages = testPages ?? new TestPages();
      _testPageStatistics = testPageStatistics ?? new TestPageStatistics();
    }

    protected override void OnProcess(InteractionAggregationPipelineArgs args)
    {
      if (!Settings.IsAutomaticContentTestingEnabled)
        return;
      Assert.ArgumentNotNull(args, nameof(args));
      var visitData = VisitDataMapper.GetVisitData(args);
      if (visitData == null || visitData.Pages == null || visitData.Pages.Count == 0)
        return;
      var fact = args.Context.Results.GetFact<Personalization>();
      foreach (var firstTimeTestPage in _testPages.GetFirstTimeTestPages(visitData.Pages))
      {
        if (firstTimeTestPage != null && firstTimeTestPage.MvTest != null && firstTimeTestPage.Item != null &&
            firstTimeTestPage.Item.Language != null && firstTimeTestPage.MvTest.EligibleRules != null)
        {
          if (!visitData.CustomValues.ContainsKey(firstTimeTestPage.MvTest.Id.ToString()))
            ProcessOldFormatInteraction(fact, visitData, firstTimeTestPage);
          else
            ProcessNewFormatInteraction(fact, visitData, firstTimeTestPage);
        }
      }
    }

    protected virtual PersonalizationKey GetKey(PageData pageData, VisitData visitData, Guid ruleSetId, Guid ruleId,
      bool isDefault = false)
    {
      Assert.ArgumentNotNull(pageData, nameof(pageData));
      Assert.ArgumentNotNull(visitData, nameof(visitData));
      return new PersonalizationKey
      {
        TestSetId = pageData.MvTest.Id,
        TestValues = pageData.MvTest.Combination,
        Date = pageData.DateTime.Date,
        RuleSetId = ruleSetId,
        RuleId = ruleId,
        IsDefault = isDefault
      };
    }

    protected virtual PersonalizationValue GetValue(PageData pageData, VisitData visitData)
    {
      Assert.ArgumentNotNull(pageData, nameof(pageData));
      Assert.ArgumentNotNull(visitData, nameof(visitData));
      var firstExposure = pageData.MvTest.FirstExposure;
      var flag = firstExposure.HasValue ? firstExposure.GetValueOrDefault() : visitData.ContactVisitIndex == 1;
      return new PersonalizationValue
      {
        Visits = 1,
        Visitors = flag ? 1L : 0L,
        Value = _testPageStatistics.GetTestValue(visitData, pageData)
      };
    }

    private void ProcessNewFormatInteraction(Personalization facts, VisitData visit, PageData page)
    {
      var customValue = (List<PersonalizationTestDataModel>) visit.CustomValues[page.MvTest.Id.ToString()];
      if (customValue == null)
        return;
      foreach (var personalizationTestDataModel in customValue)
      {
        if (personalizationTestDataModel.IsOriginalExposure && personalizationTestDataModel.EligibleRules.Any())
        {
          if (personalizationTestDataModel.EligibleRules.Count() > 1)
            personalizationTestDataModel.EligibleRules =
              personalizationTestDataModel.EligibleRules.Where(x => x != ID.Null.Guid).ToList();
          foreach (var eligibleRule in personalizationTestDataModel.EligibleRules)
          {
            var key = GetKey(page, visit, personalizationTestDataModel.RenderingId, eligibleRule,
              personalizationTestDataModel.IsOriginalExposure);
            var personalizationValue = GetValue(page, visit);
            facts.Emit(key, personalizationValue);
          }
        }
        else
        {
          var key = GetKey(page, visit, personalizationTestDataModel.RenderingId,
            personalizationTestDataModel.ExposedRule, personalizationTestDataModel.IsOriginalExposure);
          var personalizationValue = GetValue(page, visit);
          facts.Emit(key, personalizationValue);
        }
      }
    }

    private void ProcessOldFormatInteraction(Personalization facts, VisitData visit, PageData page)
    {
      var obj1 = Tracker.DefinitionDatabase?.GetItem(new ID(page.Item.Id), Language.Parse(page.Item.Language),
        Version.Parse(page.Item.Version));
      if (obj1 != null)
      {
        var obj2 = Tracker.DefinitionDatabase.GetItem(new ID(page.MvTest.Id));
        if (obj2 == null)
          return;
        var testDefinitionItem = TestDefinitionItem.Create(obj2);
        if (testDefinitionItem == null)
          return;
        var id1 = new ID(page.SitecoreDevice != null ? page.SitecoreDevice.Id : Guid.Empty);
        if (id1 == ID.Null)
          return;
        var array1 = obj1.Visualization.GetRenderings(Tracker.DefinitionDatabase.GetItem(id1), false)
          .Where(x => !string.IsNullOrEmpty(x.Settings.PersonalizationTest)).ToArray();
        if (!array1.Any())
          return;
        var testSet = TestManager.GetTestSet(new TestDefinitionItem[1]
        {
          testDefinitionItem
        }, obj1, id1);
        var testCombination = new TestCombination(page.MvTest.Combination, testSet);
        foreach (var renderingReference in array1)
        {
          var persRendering = renderingReference;
          var testVariable = testSet.Variables.FirstOrDefault(x => x.Id.Equals(Guid.Parse(persRendering.UniqueId)));
          if (testVariable == null)
          {
            Log.Warn($"Sitecore.Support.321302 rendering id {persRendering.UniqueId} is not found.", this);
            continue;
          }
          var testValue = testCombination.GetValue(Guid.Parse(persRendering.UniqueId));
          if (testValue != null)
          {
            var array2 = page.MvTest.EligibleRules.Intersect(persRendering.Settings.Rules.Rules.Select(x => x.UniqueId))
              .ToArray();
            var isDefault =
              ContentTestingFactory.Instance.TestValueInspector.IsOriginalTestValue(testVariable, testValue);
            if (isDefault && array2.Any())
            {
              if (array2.Count() > 1)
                array2 = array2.Where(x => x != ID.Null).ToArray();
              foreach (var id2 in array2)
              {
                var key = GetKey(page, visit, Guid.Parse(persRendering.UniqueId), id2.Guid, isDefault);
                var personalizationValue = GetValue(page, visit);
                facts.Emit(key, personalizationValue);
              }
            }
            else
            {
              var key = GetKey(page, visit, Guid.Parse(persRendering.UniqueId), testValue.Id, isDefault);
              var personalizationValue = GetValue(page, visit);
              facts.Emit(key, personalizationValue);
            }
          }
        }
      }
      else
      {
        foreach (var exposedRule in page.PersonalizationData.ExposedRules)
        {
          var key = GetKey(page, visit, exposedRule.RuleSetId.Guid, exposedRule.RuleId.Guid, exposedRule.IsOriginal);
          var personalizationValue = GetValue(page, visit);
          facts.Emit(key, personalizationValue);
        }
      }
    }
  }
}