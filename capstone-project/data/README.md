## Consumer Complaint Database

For this project I will use a dataset called **Consumer Complaint Database** (Consumer Finance Complaints (Bureau of Consumer Financial Protection)) which can be downloaded from Kaggle [1] or Data.gov [2].

“These are real world complaints received about financial products and services. Each complaint has been labeled with a specific product; therefore, this is a supervised text classification problem. With the aim to classify future complaints based on its content, we used different ML algorithms can make more accurate predictions (i.e., classify the complaint in one of the product categories). The dataset contains different information of complaints that customers have made about a multiple products and services in the financial sector, such us Credit Reports, Student Loans, Money Transfer, etc. The date of each complaint ranges from November 2011 to May 2019” [1].

More about dataset: https://cfpb.github.io/api/ccdb/fields.html

Publish complaints after the company responds or after 15 days, whichever comes first.


Data structure [8]:

* `Date received` - The date the CFPB received the complaint. For example, “05/25/2013.”

* `Product` - The type of product the consumer identified in the complaint. For example, “Checking or savings account” or “Student loan.”

* `Sub-product` - The type of sub-product the consumer identified in the complaint. For example, “Checking account” or “Private student loan.”

* `Issue`	- The issue the consumer identified in the complaint. For example, “Managing an account” or “Struggling to repay your loan.”

* `Sub-issue` - The sub-issue the consumer identified in the complaint. For example, “Deposits and withdrawals” or “Problem lowering your monthly payments.”

* `Consumer complaint narrative` - Consumer complaint narrative is the consumer-submitted description of “what happened” from the complaint. Consumers must opt-in to share their narrative. We will not publish the narrative unless the consumer consents, and consumers can opt-out at any time. The CFPB takes reasonable steps to scrub personal information from each complaint that could be used to identify the consumer.

* `Company public response` - The company’s optional, public-facing response to a consumer’s complaint. Companies can choose to select a response from a pre-set list of options that will be posted on the public database. For example, “Company believes complaint is the result of an isolated error.”

* `Company` - The complaint is about this company. For example, “ABC Bank.”

* `State`	- The state of the mailing address provided by the consumer.

* `ZIP code` - The mailing ZIP code provided by the consumer. This field may: i) include the first five digits of a ZIP code; ii) include the first three digits of a ZIP code (if the consumer consented to publication of their complaint narrative); or iii) be blank (if ZIP codes have been submitted with non-numeric values, if there are less than 20,000 people in a given ZIP code, or if the complaint has an address outside of the United States).

* `Tags` - Data that supports easier searching and sorting of complaints submitted by or on behalf of consumers. 
    
    For example, complaints where the submitter reports the age of the consumer as 62 years or older are tagged “Older American.” Complaints submitted by or on behalf of a servicemember or the spouse or dependent of a servicemember are tagged “Servicemember.” Servicemember includes anyone who is active duty, National Guard, or Reservist, as well as anyone who previously served and is a veteran or retiree.            

* `Consumer consent provided?` - Identifies whether the consumer opted in to publish their complaint narrative. We do not publish the narrative unless the consumer consents, and consumers can opt-out at any time.

* `Submitted via` - How the complaint was submitted to the CFPB. For example, “Web” or “Phone.”

* `Date sent to company` - The date the CFPB sent the complaint to the company.

* `Company response to consumer` - This is how the company responded. For example, “Closed with explanation.”

* `Timely response?` - Whether the company gave a timely response. For example, “Yes” or “No.”

* `Consumer disputed?` - Whether the consumer disputed the company’s response.

* `Complaint ID` - The unique identification number for a complaint.


## World population
Also, for this project I will use a 4 datasets called **World Population Review** which can be downloaded from World population [3] in  [4], [5], [6] and [7].


**References**
1. kaggle - Consumer Complaint Database. Accessed 29 Mar. 2021. Available at: https://www.kaggle.com/selener/consumer-complaint-database
2. Catalog.data.gov - Consumer Complaint Database. Accessed 29 Mar. 2021. Available at: https://catalog.data.gov/dataset/consumer-complaint-database
3. World population - States, Countries, Cities, Zip Codes. Accessed 29 Mar. 2021. Available at: https://worldpopulationreview.com
4. US-counties. Accessed 29 Mar. 2021. Available at: https://worldpopulationreview.com/us-counties
5. US-cities. Accessed 29 Mar. 2021. Available at: https://worldpopulationreview.com/us-cities
6. Zips. Accessed 29 Mar. 2021. Available at: https://worldpopulationreview.com/zips
7. States. Accessed 29 Mar. 2021. Available at: https://worldpopulationreview.com/states
8. Consumer Finance - Complaint Database. Accessed 29 Mar. 2021. Available at: https://www.consumerfinance.gov/complaint/data-use/