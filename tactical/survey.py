from datetime import date, timedelta


SURVEY_RANGE_DAYS = (7, 14, 30)


def build_survey_heatmap(rows, days=14, today=None):
    today = today or date.today()
    start_date = today - timedelta(days=days)
    date_range = [start_date + timedelta(days=i) for i in range(days)]

    results_by_survey = {}
    for survey, question, result_date, value in rows:
        result_date = (
            result_date.isoformat()
            if hasattr(result_date, "isoformat")
            else str(result_date)
        )
        results_by_survey.setdefault(survey, {}).setdefault(question, {})[
            result_date
        ] = value

    surveys = {}
    for survey, questions in results_by_survey.items():
        question_results = [
            {
                "question": question,
                "results": [
                    {"date": result_date, "value": answers.get(result_date.isoformat())}
                    for result_date in date_range
                ],
            }
            for question, answers in questions.items()
        ]

        weighted_total = 0
        total_weight = 0
        for question in question_results:
            for day_index, answer in enumerate(question["results"], start=1):
                if answer["value"] not in (None, 0):
                    weighted_total += answer["value"] * day_index
                    total_weight += day_index

        score = weighted_total / total_weight if total_weight else None
        if score is None:
            tone, summary = "neutral", "No recent responses"
        elif score < 1.5:
            tone, summary = "red", "Needs attention"
        elif score < 2.5:
            tone, summary = "yellow", "Mixed results"
        else:
            tone, summary = "green", "Strong results"

        surveys[survey] = {
            "questions": question_results,
            "score": score,
            "tone": tone,
            "summary": summary,
        }

    return date_range, surveys
