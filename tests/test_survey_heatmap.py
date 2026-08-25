from datetime import date, timedelta
import unittest

from tactical.survey import build_survey_heatmap


def result(days_ago, value, question="Question"):
    return (
        "Survey",
        question,
        date.today() - timedelta(days=days_ago),
        value,
    )


class SurveyHeatmapTest(unittest.TestCase):
    def test_range_controls_number_of_results(self):
        date_range, surveys = build_survey_heatmap([result(1, 3)], days=7)

        self.assertEqual(len(date_range), 7)
        self.assertEqual(len(surveys["Survey"]["questions"][0]["results"]), 7)

    def test_recent_answers_are_weighted_more_heavily(self):
        _, surveys = build_survey_heatmap(
            [result(13, 1), result(1, 3)], days=14
        )

        section = surveys["Survey"]
        self.assertGreater(section["score"], 2.5)
        self.assertEqual(section["tone"], "green")

    def test_empty_answers_do_not_change_section_score(self):
        _, surveys = build_survey_heatmap(
            [result(2, 0), result(1, 2)], days=14
        )

        self.assertEqual(surveys["Survey"]["score"], 2)
        self.assertEqual(surveys["Survey"]["tone"], "yellow")


if __name__ == "__main__":
    unittest.main()
